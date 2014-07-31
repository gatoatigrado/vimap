# -*- coding: utf-8 -*-
'''
The real worker routine used by vimap.pool. This provides a runnable which
consumes from an input queue, and enqueues results to an output queue.

TODO: Change debug print to use `logging`. This will require some real
testing though...
'''
from __future__ import absolute_import
from __future__ import print_function

import multiprocessing
import multiprocessing.queues
import os
import sys

import vimap.exception_handling


_IDLE_TIMEOUT = 0.02


class WorkerRoutine(object):
    def __init__(self, fcn, init_args, init_kwargs, index, debug):
        self.fcn = fcn
        self.init_args = init_args
        self.init_kwargs = dict(init_kwargs)
        self.index, self.debug_enabled = index, debug

    def debug(self, message, *fmt_args, **fmt_kwargs):
        if self.debug_enabled:
            print("Worker[{0}, {pid}] {msg}".format(
                self.index,
                pid=multiprocessing.current_process().pid,
                msg=message.format(*fmt_args, **fmt_kwargs))
            )

    def worker_input_generator(self):
        '''Call this on the worker processes: yields input.'''
        while True:
            try:
                x = self.input_queue.get(timeout=_IDLE_TIMEOUT)
                if x is None:
                    return
                if self.input_index is not None:
                    vimap.exception_handling.print_warning(
                        "Didn't produce an output for input!",
                        input_index=self.input_index)
                self.input_index, z = x
                self.debug("Got input #{0}", self.input_index)
                yield z
            except multiprocessing.queues.Empty:
                # print("Waiting")
                pass
            except IOError:
                print(
                    "Worker error getting item from input queue",
                    file=sys.stderr)
                raise

    def safe_close_queue(self, name, queue):
        self.debug("Closing queue {0}", name)
        queue.close()
        try:
            self.debug("Joining thread for queue {0}", name)

            try:
                self.debug(
                    "Joining queue {name} (size {size}, full: {full})",
                    name=name,
                    size=queue.qsize(),
                    full=queue.full())
            except NotImplementedError:
                pass  # Mac OS X doesn't implement qsize()
            queue.join_thread()
        # threads might have already been closed
        except AssertionError as e:
            self.debug("Couldn't join queue {0}; error {1}", name, e)
        else:
            self.debug("Done closing {0}, no exceptions.", name)

    def explicitly_close_queues(self):
        '''Explicitly join queues, so that we'll get "stuck" in something that's
        more easily debugged than multiprocessing.

        NOTE: It's tempting to call self.output_queue.cancel_join_thread(),
        but this seems to leave us in a bad state in practice (reproducible
        via existing tests).
        '''
        self.safe_close_queue('input', self.input_queue)
        self.safe_close_queue('output', self.output_queue)

    def handle_output(self, output):
        """Makes the imperative calls to put an output item on the output
        queue, and does some sanity checks to make sure the function is
        yielding only one output for every input.
        """
        assert self.input_index is not None, (
            "Produced output before getting first input, or multiple "
            "outputs for one input. Output: {0}".format(output))
        self.debug("Produced output for input #{0}", self.input_index)
        self.output_queue.put((self.input_index, 'output', output))
        self.input_index = None  # prevent it from producing mult. outputs

    def run(self, input_queue, output_queue):
        '''
        Takes ordered items from input_queue, lets `fcn` iterate over
        those, and puts items yielded by `fcn` onto the output queue,
        with their IDs.
        '''
        self.input_queue, self.output_queue = input_queue, output_queue
        self.input_index = None
        self.debug("starting; PID {0}", os.getpid())
        try:
            fcn_iter = self.fcn(self.worker_input_generator(), *self.init_args, **self.init_kwargs)
            try:
                iter(fcn_iter)
            except TypeError:
                vimap.exception_handling.print_warning(
                    "Your worker function must yield values for inputs it consumes!",
                    fcn_return_value=fcn_iter)
                assert False
            for output in fcn_iter:
                self.handle_output(output)
        except Exception:
            ec = vimap.exception_handling.ExceptionContext.current()
            self.debug('{0}', ec.formatted_traceback)
            self.output_queue.put((self.input_index, 'exception', ec))

        self.explicitly_close_queues()
        self.debug("exiting")
