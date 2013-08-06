import time

import testify as T

import vimap.pool
import vimap.worker_process


@vimap.worker_process.worker
def worker_proc(seq, init=0, sleep_s=None):
    for x in seq:
        if sleep_s is not None:
            time.sleep(sleep_s)
        yield x + init


class FuzzTest(T.TestCase):
    def test_fuzz(self):
        for n in xrange(1, 100):
            processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, 1, 1])
            res = list(processes.imap(list(range(1, n))).zip_in_out())
            T.assert_equal(set(out for in_, out in res), set(range(2, n+1)))

    def test_try_overwhelm_output_queue(self):
        processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, 1, 1])
        processes.imap(xrange(10000)).block_ignore_output()

    def test_slow_consumer(self):
        processes = vimap.pool.fork(worker_proc.init_args(
            init=i, sleep_s=0.001) for i in [1] * 10)
        for _, output in processes.imap(xrange(100)).zip_in_out():
            time.sleep(0.01)
