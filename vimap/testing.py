'''
Provides methods for tests.
'''
from collections import namedtuple
import itertools
import multiprocessing
import Queue

import mock

import vimap.exception_handling
import vimap.pool

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
    """A process that doesn't actually fork."""

    def start(self):
        pass

    def join(self):
        pass


class SerialQueueManager(vimap.queue_manager.VimapQueueManager):
    queue_class = Queue.Queue
    # hack
    queue_class.close = lambda self: None
    queue_class.join_thread = lambda self: None

    def pop_output(self):
        self.num_real_in_flight -= 1
        return super(SerialQueueManager, self).pop_output()


class SerialPool(DebugPool):
    """A pool that processes input serially.

    This makes attaching debuggers to worker processes easier.
    """
    process_class = SerialProcess
    queue_manager_class = SerialQueueManager

    def spool_input(self, close_if_done=True):
        # Throw some `None`s onto the queue to stop workers
        def inputs():
            for serialized_input in self.all_input_serialized:
                yield serialized_input
                yield None

        self.qm.spool_input(inputs())

        # simulate parallelism by dispatching work to each of our workers.
        workers = itertools.cycle(self.processes)

        while not self.qm.input_queue.empty():
            worker_proc = workers.next()
            worker_proc._target(*worker_proc._args, **worker_proc._kwargs)


def mock_debug_pool():
    return mock.patch.object(vimap.pool, 'VimapPool', DebugPool)

def mock_serial_pool():
    return mock.patch.object(vimap.pool, 'VimapPool', SerialPool)

