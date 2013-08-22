'''
Provides methods for tests.
'''
from collections import namedtuple
import itertools
import multiprocessing

import mock

import vimap.exception_handling
import vimap.pool
import vimap.real_worker_routine

DebugResult = namedtuple('DebugResult', ['uid', 'input', 'output'])

get_func = lambda x: lambda y: x + y
unpickleable = (get_func(3), 3)


def no_warnings():
    '''Make vimap.exception_handling.print_warning fail tests.'''
    import testify as T # in case you're not using testify

    return mock.patch.object(vimap.exception_handling, 'print_warning',
        lambda *args, **kwargs: T.assert_not_reached())


class DebugPool(vimap.pool.VimapPool):
    def __init__(self, *args, **kwargs):
        super(DebugPool, self).__init__(*args, **kwargs)
        self.debug_results = []

    @property
    def output_for_input(self):
        return dict((r.input, r.output) for r in self.debug_results)

    def get_corresponding_input(self, uid, output):
        '''Dummy method for mocking.'''
        input_ = super(DebugPool, self).get_corresponding_input(uid, output)
        self.debug_results.append(DebugResult(uid, input_, output))
        return input_


class SerialProcess(multiprocessing.Process):
    '''A process that doesn't actually fork.'''

    def start(self):
        pass

    def join(self):
        pass


class SerialWorkerRoutine(vimap.real_worker_routine.WorkerRoutine):
    '''A routine that doesn't need queues for input/output.'''

    def explicitly_close_queues(self):
        '''Don't close queues, since we haven't actually forked!'''
        pass

    def worker_input_generator(self):
        '''Only step our workers once.'''
        try:
            self.input_index, next_input = self.input_queue.get()
            yield next_input
        except TypeError:
            return


class SerialPool(DebugPool):
    '''A pool that processes input serially.

    This pool does not fork. This makes attaching debuggers to worker processes
    easier.

    The pool will spool input to workers in cyclical order, to simulate how
    work might be distributed in the multi-process case.
    '''
    process_class = SerialProcess
    worker_routine_class = SerialWorkerRoutine

    def spool_input(self, close_if_done=True):
        self.qm.spool_input(self.all_input_serialized)

        workers = itertools.cycle(self.processes)

        while not self.qm.input_queue.empty():
            worker_proc = workers.next()
            worker_proc._target(*worker_proc._args, **worker_proc._kwargs)

        self.finish_workers()


def mock_debug_pool():
    return mock.patch.object(vimap.pool, 'VimapPool', DebugPool)

def mock_serial_pool():
    return mock.patch.object(vimap.pool, 'VimapPool', SerialPool)

