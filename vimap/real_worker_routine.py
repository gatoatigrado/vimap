'''
The real worker routine used by vimap.pool. This provides a runnable which
consumes from an input queue, and enqueues results to an output queue.
'''
from __future__ import absolute_import
from __future__ import print_function

import multiprocessing.queues
import os
import sys
import traceback

import vimap.exception_handling


#debug = print
debug = lambda *args, **kwargs: None


_IDLE_TIMEOUT = 0.02

class WorkerRoutine(object):
    def __init__(self, fcn, init_args, init_kwargs):
        self.fcn = fcn
        self.init_args = init_args
        self.init_kwargs = dict(init_kwargs)
        self.init_kwargs_str = str(self.init_kwargs) # for debug printing

    def debug(self, message, *fmt_args, **fmt_kwargs):
        debug("Worker[pid {0}, kwargs {1}] {2}".format(
            os.getpid(), self.init_kwargs_str, message.format(*fmt_args, **fmt_kwargs)))

    def worker_input_generator(self):
        '''Call this on the worker processes: yields input.'''
        while True:
            try:
                x = self.input_queue.get(timeout=_IDLE_TIMEOUT)
                # print("Got {0} from input queue.".format(x))
                if x is None:
                    return
                if self.input_index is not None:
                    vimap.exception_handling.print_warning(
                        "Didn't produce an output for input!",
                        input_index=self.input_index)
                self.input_index, z = x
                yield z
            except multiprocessing.queues.Empty:
                # print("Waiting")
                pass
            except IOError:
                print("Worker error getting item from input queue",
                    file=sys.stderr)
                raise

    def explicitly_close_queues(self):
        '''Explicitly join queues, so that we'll get "stuck" in something that's
        more easily debugged than multiprocessing.

        NOTE: It's tempting to call self.output_queue.cancel_join_thread(),
        but this seems to leave us in a bad state in practice (reproducible
        via existing tests).
        '''
        self.input_queue.close()
        self.output_queue.close()
        try:
            self.debug("Joining input queue")
            self.input_queue.join_thread()
            self.debug("...done")

            try:
                self.debug("Joining output queue (size {size}, full: {full})",
                    size=self.output_queue.qsize(),
                    full=self.output_queue.full())
            except NotImplementedError: pass # Mac OS X doesn't implement qsize()
            self.output_queue.join_thread()
            self.debug("...done")
        # threads might have already been closed
        except AssertionError: pass

    def run(self, input_queue, output_queue):
        '''
        Takes ordered items from input_queue, lets `fcn` iterate over
        those, and puts items yielded by `fcn` onto the output queue,
        with their IDs.
        '''
        self.input_queue, self.output_queue = input_queue, output_queue
        self.input_index = None
        self.debug("starting")
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
                assert self.input_index is not None, (
                    "Produced output before getting first input, or multiple "
                    "outputs for one input. Output: {0}".format(output))
                self.output_queue.put( (self.input_index, 'output', output) )
                self.input_index = None # prevent it from producing mult. outputs
        except Exception as e:
            self.debug("Got exception {0}\n{1}", e, traceback.format_exc())
            self.output_queue.put( (self.input_index, 'exception', e) )

        self.explicitly_close_queues()
        self.debug("exiting")
