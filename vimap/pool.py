'''
Provides process pools for vimap.

TBD:

For more complex tasks, which we might want to handle exceptions,

    def process_result(input):
        try:
            result = (yield)
            print("For input {0} got result {1}".format(input, result)
        except Exception as e:
            print("While processing input {0}, got exception {1}".format(input, e))

    processes.imap(entire_input_sequence).handle_result(process_result)

You can also use it in a more "async" manner, e.g. when your input sequences are
relatively small and/or calculated ahead of time, you can write,

    processes.map(seq1)
    processes.map(seq2)

(by default, input is only enqueued as results are consumed.)
'''
from __future__ import absolute_import
from __future__ import print_function

import itertools
import multiprocessing
import multiprocessing.queues
import signal
import sys
import time
import traceback

import vimap.exception_handling
import vimap.real_worker_routine


# TODO: Find why this is necessary
_MAX_IN_FLIGHT = 100



class Imap2Pool(object):
    '''Args: Sequence of imap2 workers.'''

    # TODO: Implement timeout in joining workers
    #
    def __init__(self, worker_sequence, in_queue_size_factor=10, timeout=5.0):
        self.in_queue_size_factor = in_queue_size_factor
        self.worker_sequence = list(worker_sequence)

        self._input_queue = multiprocessing.Queue(self.max_in_flight)
        self._output_queue = multiprocessing.Queue(self.max_in_flight)
        self.num_inflight = 0

        self.processes = []

        self.timeout = timeout

        self.input_uid_ctr = 0
        self.input_uid_to_input = {} # input to keep around until handled
        self.input_sequences = []
        self.finished_workers = False

    max_in_flight = property(lambda self: min(_MAX_IN_FLIGHT,
        self.in_queue_size_factor * len(self.worker_sequence)))

    def fork(self):
        for worker in self.worker_sequence:
            routine = vimap.real_worker_routine.WorkerRoutine(
                worker.fcn, worker.args, worker.kwargs)
            process = multiprocessing.Process(
                target=routine.run,
                args=(self._input_queue, self._output_queue))
            process.daemon = True # processes will be controlled by parent
            process.start()
            self.processes.append(process)

    def __del__(self):
        '''Don't hang if all references to the pool are lost.'''
        self.finish_workers()
        self.consume_all_output_print_errors()

    def put_input(self, x):
        '''NOTE: This might raise an exception if the workers have died during
        initialization on Linux. It's currently reproducible via the
        ExceptionsTest.test_basic_exceptions; I'm not sure how to fix it.
        '''
        self.num_inflight += 1
        self._input_queue.put(x)

    def pop_output(self):
        rv = self._output_queue.get_nowait()
        self.num_inflight -= 1 # only decrement if no exceptions were thrown
        return rv

    def consume_all_output_print_errors(self):
        '''Pull all output off the output queue, print any exceptions.

        This is useful if something crashed on the main thread, before
        worker exceptions were printed.
        '''
        while not self._output_queue.empty():
            try:
                uid, typ, output = self.pop_output()
                if typ == 'exception':
                    vimap.exception_handling.print_exception(output, None, None)
            except multiprocessing.queues.Empty:
                time.sleep(0.01)

    def finish_workers(self):
        '''Sends stop tokens to subprocesses, then joins them. There may still be
        unconsumed output.
        '''
        if not self.finished_workers:
            for _ in self.processes:
                self._input_queue.put(None)
            for process in self.processes:
                process.join()
            self.finished_workers = True

    # === Input-enqueueing functionality
    def imap(self, input_sequence, pretransform=False):
        '''Spools bits of an input sequence to workers' queues; good
        for doing things like iterating through large files, live
        inputs, etc. Otherwise, use map.

        Keyword arguments:
            pretransform -- if True, then assume input_sequence items
                are pairs (x, tf(x)), where tf is some kind of
                pre-serialization transform, applied to input elements
                before they are sent to worker processes.
        '''
        if pretransform:
            self.input_sequences.append(iter(input_sequence))
        else:
            self.input_sequences.append(((v, v) for v in input_sequence))
        self.spool_input(close_if_done=False)
        return self

    # NOTE: `map` may overwhelm the output queue and cause things to freeze,
    # therefore it's getting removed for now.
    #
    # def map(self, *args, **kwargs):
    #     '''Like `imap`, but adds the entire input sequence.'''
    #     self.imap(*args, **kwargs).enqueue_all()
    #     return self

    @property
    def all_input(self):
        '''Input from all calls to imap; downside of this approach
        is that it keeps around dead iterators.
        '''
        return (x for seq in self.input_sequences for x in seq)

    def spool_input(self, close_if_done=True):
        '''Put input on the queue. Spools enough input for twice the
        number of processes.
        '''
        n_to_put = self.max_in_flight - self.num_inflight

        if n_to_put > 0:
            inputs = list(itertools.islice(self.all_input, n_to_put))
            for x in inputs:
                self.enqueue(x)
            if close_if_done and (not inputs):
                self.finish_workers()

    def enqueue(self, (x, xser)):
        '''
        Arguments:
            x -- the real input element
            xser -- the input element to be serialized and sent
                to the worker process
        '''
        uid = self.input_uid_ctr
        self.input_uid_ctr += 1
        self.input_uid_to_input[uid] = x

        try:
            self.put_input((uid, xser))
        except IOError:
            print("Error enqueueing item from main process", file=sys.stderr)
            raise

    def enqueue_all(self):
        '''Enqueue all input sequences assigned to this pool.'''
        for x in self.all_input:
            self.enqueue(x)
    # ------

    # === Results-consuming functions
    def zip_in_out(self, close_if_done=True):
        def has_output_or_inflight():
            '''returns True if there are processes alive, or items on the
            output queue.
            '''
            return (not self._output_queue.empty()) or (
                (sum(p.is_alive() for p in self.processes) > 0))

        self.spool_input(close_if_done=close_if_done)

        while self.input_uid_to_input and has_output_or_inflight():
            self.spool_input(close_if_done=close_if_done)
            try:
                uid, typ, output = self.pop_output()
                if typ == 'output':
                    yield self.input_uid_to_input.pop(uid), output
                elif typ == 'exception':
                    vimap.exception_handling.print_exception(output, None, None)
            except multiprocessing.queues.Empty:
                time.sleep(0.01)
            except IOError:
                print("Error getting output queue item from main process",
                    file=sys.stderr)
                raise
        if close_if_done:
            self.finish_workers()
        # Return when input given is exhausted, or workers die from exceptions
    # ------

    def ignore_output(self, *args, **kwargs):
        for _ in self.zip_in_out(*args, **kwargs): pass

def fork(*args, **kwargs):
    pool = Imap2Pool(*args, **kwargs)
    pool.fork()
    return pool


def unlabeled(worker_fcn, *args, **kwargs):
    '''Shortcut for when you don't care about per-worker initialization
    arguments.

    Example usage:

        parse_mykey = imap2.unlabeled_pool(
            lambda line: simplejson.loads(line)['mykey'])
        entries = parse_mykey.imap(fileinput.input())
    '''
    num_workers = kwargs.pop('num_workers', None)
    if num_workers is None:
        num_workers = multiprocessing.cpu_count()
    return fork(worker_fcn.init_args(*args, **kwargs)
        for _ in range(num_workers))
