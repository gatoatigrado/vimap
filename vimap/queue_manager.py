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


# TODO: Find why this is necessary
# (in practice, it seems some things stall otherwise)
_MAX_IN_FLIGHT = 100


class VimapQueueManager(object):
    '''Args: Sequence of vimap workers.'''
    queue_class = multiprocessing.queues.Queue

    def __init__(self, max_real_in_flight, max_total_in_flight, debug=False):
        '''
        Arguments:
            max_real_in_flight -- number of in-flight operations sent to
                worker processes
            max_total_in_flight -- number of in-flight operations including
                temporary master-process output queue
        '''
        self.max_real_in_flight = min(_MAX_IN_FLIGHT, max_real_in_flight)
        self.max_total_in_flight = max_total_in_flight
        self.input_queue = self.queue_class(max_real_in_flight)
        self.output_queue = self.queue_class(max_real_in_flight)

        # Temporary output queue (enqueue with `append`, dequeue with `pop(0)`).
        # Prevents threads from blocking
        self.tmp_output_queue = []

        # Number of in-flight operations in queues, not counting the
        # temporary output queue
        self.num_real_in_flight = 0

        self.output_hooks = []
        self.debug = debug

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
        self.input_queue.put(x)
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
        '''
        start_time = time.time()
        while (max_time_s is None) or (time.time() - start_time < max_time_s):
            try:
                item = self.output_queue.get_nowait()
                self.num_real_in_flight -= 1  # only decrement if no exceptions were thrown

                if self.debug:
                    print("Main thread: got item #{0}".format(item[0]))
                for hook in self.output_hooks:
                    hook(item)
                self.tmp_output_queue.append(item)
            except multiprocessing.queues.Empty:
                break

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
            self.max_total_in_flight - self.num_total_in_flight)

        if n_to_put > 0:
            inputs = list(itertools.islice(input_iterator, n_to_put))
            for x in inputs:
                self.put_input(x)
            if not inputs:
                return True
