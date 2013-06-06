import testify as T

import vimap.pool
import vimap.worker_process


@vimap.worker_process.worker
def worker_proc(seq, init=0):
    for x in seq:
        yield x + init


class FuzzTest(T.TestCase):
    def test_fuzz(self):
        for n in xrange(1, 100):
            processes = vimap.pool.fork(worker_proc.init_args(init=i) for i in [1, 1, 1])
            res = list(processes.imap(list(range(1, n))).zip_in_out())
            T.assert_equal(set(out for in_, out in res), set(range(2, n+1)))
