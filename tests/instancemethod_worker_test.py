import cPickle
import time

import testify as T

import vimap.pool
import vimap.worker_process
from vimap.testing import unpickleable


class TestInstance(object):
    def __init__(self, x):
        self.x = x

    @vimap.worker_process.instancemethod_worker
    def worker(self, iterable, init_arg):
        for z in iterable:
            time.sleep(0.03)
            yield self.x[1] + z + init_arg


class InstancemethodWorkerTest(T.TestCase):
    def test_basic(self):
        ti = TestInstance(unpickleable)
        pool = vimap.pool.fork(ti.worker.init_args(init_arg=4) for _ in xrange(3))
        result = list(pool.imap([2100, 2200, 2300]).zip_in_out())
        T.assert_equal(set(result), set([(2300, 2307), (2100, 2107), (2200, 2207)]))

    def test_unpickleable(self):
        T.assert_raises(
            TypeError,
            lambda: cPickle.dumps(unpickleable))
