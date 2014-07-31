'''
Manages input and output queues. vimap attempts to spool data into the input
queue, so workers are fed, but of course not put all the data onto the input
queue immediately (e.g. if we're iterating over a large file). Similarly,
vimap tries to consume output from the processes, but has a bound on that
too, to keep the in-memory dataset bounded.
'''
from __future__ import absolute_import
from __future__ import print_function

import itertools
import multiprocessing
import multiprocessing.queues
import time
from vimap import exception_handling

import vimap.util


# TODO: Find why this is necessary
# (in practice, it seems some things stall otherwise)
_MAX_IN_FLIGHT = 100


class VimapQueueManager(object):
    '''Args: Sequence of vimap workers.'''
    queue_class = multiprocessing.queues.Queue

    def __init__(self, max_real_in_flight, max_total_in_flight, timeouts_config, debug=False):
        '''
        Arguments:
            max_real_in_flight -- number of in-flight operations sent to
                worker processes
            max_total_in_flight -- number of in-flight operations including
                temporary master-process output queue
        '''
        self.timeouts_config = timeouts_config
        self.max_real_in_flight = min(_MAX_IN_FLIGHT, max_real_in_flight)
        self.max_total_in_flight = max_total_in_flight
        self.input_queue = self.queue_class(max_real_in_flight)
        self.output_queue = self.queue_class(max_real_in_flight)

        # Temporary input queue
        self.tmp_input_queue = []

        # Temporary output queue (enqueue with `append`, dequeue with `pop(0)`).
        # Prevents threads from blocking
        self.tmp_output_queue = []

        # Number of in-flight operations in queues, not counting the
        # temporary output queue
        self.num_real_in_flight = 0

        self.output_hooks = []
        self.debug = debug

    @vimap.util.instancemethod_runonce()
    def close(self):
        """
        Closes any queues from the main method side. For input and output
        queues, we will close the queue, join the queue thread, and delete
        the corresponding attribute so any future attempted accesses will
        fail.
        """
        finalize_methods = []

        def _wait_close(queue_name, pipe_name, pipe):
            """Works around bugs (or misuse?) in multiprocessing.queue by waiting for
            a queue's internal pipes to actually be closed.

            See https://github.com/gatoatigrado/vimap/issues/14 for more information.
            """
            if not pipe.closed:
                if self.debug:
                    print("Force-closing {0} pipe for queue {1}".format(pipe_name, queue_name))
                pipe.close()

        def _close_queue(name, queue):
            if self.debug:
                print("Main thread queue manager: Closing and joining {0} queue".format(name))
            queue.close()
            queue.join_thread()

            # NOTE: If we're using a different queue (e.g. our mock SerialQueue),
            # or using a different implementation of Python, don't do this fragile
            # mock.
            if hasattr(queue, '_reader') and hasattr(queue, '_writer'):
                reader_pipe, writer_pipe = queue._reader, queue._writer
                finalize_methods.append(lambda: _wait_close(name, 'reader', reader_pipe))
                finalize_methods.append(lambda: _wait_close(name, 'writer', writer_pipe))

        _close_queue('input', self.input_queue)
        del self.input_queue  # Make future accesses fail

        assert self.output_queue.empty(), (
            "You should *not* close the output queue before it's all "
            "consumed, else any workers putting items into the queuewill hang!")
        _close_queue('output', self.output_queue)
        del self.output_queue  # Make future accesses fail

        for finalize_method in finalize_methods:
            finalize_method()

    def add_output_hook(self, hook):
        '''Add a function which will be executed immediately when output is
        taken off of the queue. The only current use case is to react to
        exceptions.
        '''
        self.output_hooks.append(hook)

    @property
    def num_total_in_flight(self):
        return self.num_real_in_flight + len(self.tmp_output_queue)

    def put_input(self, x):
        self.input_queue.put(x, timeout=self.timeouts_config.input_queue_put_timeout)
        self.num_real_in_flight += 1

    def feed_out_to_tmp(self, max_time_s=None):
        '''Feeds output to temporary queue "heuristically". We're always
        guaranteed to take the first element, if it exists at the time
        feed_out_to_tmp is called.  After that, if it's longer than
        `max_time_s`, we exit. If `max_time_s` is None, then we'll take
        all elements on the output queue.

        Currently, this method is called by pop_output, spool_input, and
        the Pool's finish_workers. All of these methods are called once
        in the loop of getting output elements, so it's okay if we don't
        dequeue all elements from the queue (or more arrive later).

        The use of `max_time_s` is to keep things "streaming" -- if it takes
        a long time to transfer data back, it might be worth skipping out
        of here and continuing the main process, so it can e.g. queue more
        input and keep workers busy.

        :returns: True if it got a new item, False otherwise
        :rtype: bool
        '''
        start_time = time.time()
        got_item = False
        while (max_time_s is None) or (time.time() - start_time < max_time_s):
            try:
                item = self.output_queue.get_nowait()
                self.num_real_in_flight -= 1  # only decrement if no exceptions were thrown

                self.tmp_output_queue.append(item)
                got_item = True

                if self.debug:
                    print("Main thread: got item #{0}".format(item[0]))
                for hook in self.output_hooks:
                    hook(item)
            except multiprocessing.queues.Empty:
                break
        return got_item

    def pop_output(self):
        '''Essentially a buffered version of output_queue.get_nowait().'''
        if self.debug:
            print("Main thread: feeding output --> tmp ...")
        self.feed_out_to_tmp(max_time_s=1)
        if self.debug:
            print("... Main thread done feeding output")
        if self.tmp_output_queue:
            return self.tmp_output_queue.pop(0)
        else:
            raise multiprocessing.queues.Empty()

    def spool_input(self, input_iterator):
        '''
        Put input from `input_iterator` on the input queue. Spools as many
        as permitted by max_real_in_flight and max_total_in_flight allow.

        Returns:
            True iff `input_iterator` is exhausted.
        '''
        self.feed_out_to_tmp()
        n_to_put = min(
            self.max_real_in_flight - self.num_real_in_flight,
            self.max_total_in_flight - self.num_total_in_flight
        )

        if n_to_put > 0:
            self.tmp_input_queue += list(
                itertools.islice(input_iterator, n_to_put - len(self.tmp_input_queue))
            )

            if not self.tmp_input_queue:
                return True
            try:
                while self.tmp_input_queue:
                    x = self.tmp_input_queue[0]
                    self.put_input(x)
                    self.tmp_input_queue.pop(0)
            except multiprocessing.queues.Full:
                pass

    def send_stop_tokens(self, num_tokens):
        try:
            for _ in xrange(num_tokens):
                self.input_queue.put(None, timeout=self.timeouts_config.general_timeout)
        except multiprocessing.queues.Full:
            exception_handling.print_warning(
                "Failed to send all stop tokens (timeout exceeded)"
            )
