import testify as T

import vimap.pool
import vimap.worker_process
from vimap.testing import unpickleable


@vimap.worker_process.worker
def worker_proc(seq, init):
    for x in seq:
        yield x + init[1]


class FuzzTest(T.TestCase):
    def test_unpickleable_init(self):
        processes = vimap.pool.fork(worker_proc.init_args(init=unpickleable) for i in [1, 1, 1])
        T.assert_equal(list(processes.imap([1]).zip_in_out()), [(1, 4)])
