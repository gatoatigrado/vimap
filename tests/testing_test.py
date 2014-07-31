import mock
import testify as T

import vimap.pool
import vimap.testing
import vimap.worker_process


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
        processes.imap([1, 2, 3])
        processes.block_ignore_output()

        for i in [1, 2, 3]:
            T.assert_equal(processes.output_for_input[i], i + 1)

        assert not start.called

    def test_zip_in_out(self):
        processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, 2, 3])

        results = list(processes.imap([4, 4, 4]).zip_in_out(close_if_done=False))
        T.assert_sets_equal(set(results), set([(4, 5), (4, 6), (4, 7)]))

        results = list(processes.imap([4, 4, 4]).zip_in_out(close_if_done=True))
        T.assert_sets_equal(set(results), set([(4, 5), (4, 6), (4, 7)]))

    def test_zip_in_out_lots_of_input(self):
        processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, 2, 3])
        results = list(processes.imap([4, 4, 4] * 3).zip_in_out())
        T.assert_equal(set(results[0:3]), set([(4, 5), (4, 6), (4, 7)]))
        T.assert_equal(set(results[3:6]), set([(4, 5), (4, 6), (4, 7)]))
        T.assert_equal(set(results[6:9]), set([(4, 5), (4, 6), (4, 7)]))

    def test_zip_in_out_more_workers_than_input(self):
        processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, -1, 0])
        results = list(processes.imap([4, 4]).zip_in_out())
        T.assert_equal(set(results), set([(4, 5), (4, 3)]))

    def test_zip_in_out_no_input(self):
        processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, -1, 0])
        results = processes.imap([]).zip_in_out()
        T.assert_equal(set(results), set([]))


class NoWarningsTest(T.TestCase):

    def test_no_warnings(self):
        with mock.patch('sys.stderr') as stderr:
            vimap.exception_handling.print_warning('')
            T.assert_equal(True, stderr.write.called)
            T.assert_equal(True, stderr.flush.called)

        with T.assert_raises(AssertionError):
            with vimap.testing.no_warnings():
                vimap.exception_handling.print_warning('')

if __name__ == '__main__':
    T.run()
