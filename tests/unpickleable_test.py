import cPickle
import testify as T

import vimap.pool
import vimap.worker_process
from vimap.testing import unpickleable


@vimap.worker_process.worker
def worker_proc(seq, init):
    assert init is unpickleable

    for x in seq:
        yield x + init[1]


class FuzzTest(T.TestCase):
    def test_unpickleable_init_kwarg(self):
        """Test that we can pass anything as init_args, in kwarg format.
        """
        processes = vimap.pool.fork(
            worker_proc.init_args(init=unpickleable)
            for i in [1, 1, 1])
        T.assert_equal(list(processes.imap([1]).zip_in_out()), [(1, 4)])

    def test_unpickleable_init_arg(self):
        """Test that we can pass anything as init_args, in positional argument format.
        """
        processes = vimap.pool.fork(
            worker_proc.init_args(unpickleable)
            for i in [1, 1, 1])
        T.assert_equal(list(processes.imap([1]).zip_in_out()), [(1, 4)])

    def test_really_unpickleable(self):
        """Check that the unpickleable object can't be serialized by cPickle.
        """
        with T.assert_raises(TypeError):
            cPickle.dumps(unpickleable)

    def test_unprintable(self):
        """Check that the unpickleable object can't be converted to a string either.
        """
        with T.assert_raises(TypeError):
            str(unpickleable)
