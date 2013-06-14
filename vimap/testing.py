'''
Provides methods for tests.
'''
import mock
import multiprocessing
from collections import namedtuple

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


def mock_debug_pool():
    return mock.patch.object(vimap.pool, 'VimapPool', DebugPool)
