import mock
import testify as T

import vimap.pool
import vimap.worker_process
import vimap.testing


@vimap.worker_process.worker
def worker_proc(seq, init):
    for x in seq:
        yield x + init


class DebugPoolTest(T.TestCase):

    @vimap.testing.mock_debug_pool()
    def test_debug_pool(self):
        T.assert_equal(vimap.pool.VimapPool, vimap.testing.DebugPool)
        processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, 1, 1])
        processes.imap([1, 2, 3]).block_ignore_output()

        for i in [1, 2, 3]:
            T.assert_equal(processes.output_for_input[i], i + 1)


class SerialPoolTest(T.TestCase):

    @T.setup_teardown
    def mock_pool(self):
        with vimap.testing.mock_serial_pool():
            T.assert_equal(vimap.pool.VimapPool, vimap.testing.SerialPool)
            yield

    @mock.patch('multiprocessing.Process.start')
    def test_serial_pool_doesnt_fork(self, start):
        processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, 1, 1])
        processes.imap([1, 2, 3]).block_ignore_output()

        for i in [1, 2, 3]:
            T.assert_equal(processes.output_for_input[i], i + 1)

        assert not start.called




if __name__ == '__main__':
    T.run()
