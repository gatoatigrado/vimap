import testify as T

import vimap.pool
import vimap.worker_process
import vimap.testing


@vimap.worker_process.worker
def worker_proc(seq, init):
    for x in seq:
        yield x + init


class DebugPoolTest(T.TestCase):
    def test_debug_pool(self):
        with vimap.testing.mock_debug_pool():
            T.assert_equal(vimap.pool.VimapPool, vimap.testing.DebugPool)
            processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, 1, 1])
            processes.imap([1, 2, 3]).block_ignore_output()
            for i in [1, 2, 3]:
                T.assert_equal(processes.output_for_input[i], i + 1)
