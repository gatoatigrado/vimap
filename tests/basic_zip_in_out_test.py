import time

import mock
import testify as T

import vimap.pool
import vimap.worker_process


@vimap.worker_process.worker
def worker_proc(seq, init=0):
    for x in seq:
        time.sleep(0.03)
        yield x + init


class BasicInoutTest(T.TestCase):
    @mock.patch('multiprocessing.Process')
    def test_no_hang(self, process_mock):
        '''Test that nothing hangs if the processes are never used.

        The mechanics of this test are tricky: We make it such that a process
        pool is deleted, but this process pool has an overridden method that
        tells our test class that it actually cleaned up workers.
        '''
        test_passes = {}
        processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, 2, 3])
        processes.finish_workers = lambda: test_passes.setdefault('result', True)
        del processes  # will happen if it falls out of scope

        # gc.collect() -- doesn't seem necessary
        T.assert_dicts_equal(test_passes, {'result': True})

    def test_basic(self):
        processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, 2, 3])
        list(processes.zip_in_out())

    def test_reuse_pool(self):
        '''
        Test that process pools can be re-used. This is important for avoiding
        forking costs.
        '''
        processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, 2, 3])

        results = list(processes.imap([4, 4, 4]).zip_in_out(close_if_done=False))
        assert set(results) == set([(4, 5), (4, 6), (4, 7)])

        results = list(processes.imap([4, 4, 4]).zip_in_out(close_if_done=True))
        assert set(results) == set([(4, 5), (4, 6), (4, 7)])

    def test_really_parallel(self):
        '''Make sure things run in parallel: Determine that different threads are
        handling different inputs (via time.sleep stuff). This could fail if the
        sleep values are too small to compensate for the forking overhead.
        '''
        processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, 2, 3])
        results = []
        for input, output in processes.imap([4, 4, 4] * 3).zip_in_out():
            results.append((input, output))
            time.sleep(0.06)
        T.assert_equal(set(results[0:3]), set([(4, 5), (4, 6), (4, 7)]))
        T.assert_equal(set(results[3:6]), set([(4, 5), (4, 6), (4, 7)]))
        T.assert_equal(set(results[6:9]), set([(4, 5), (4, 6), (4, 7)]))
