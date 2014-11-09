from __future__ import absolute_import
from __future__ import print_function

import testify

import vimap.api.v1 as vimap


__author__ = 'gatoatigrado'


@vimap.fcn_worker
def process(x):
    return x * 2


@vimap.fcn_worker
def process_with_conn(x, conn):
    return conn.post(x)


class MockConnection(object):
    """
    In real life this would be something like `requests.Session`,
    an ElasticSearch connection, etc.

    For here, it's just something that multiplies numbers.
    """
    def __init__(self, multiplier):
        self.multiplier = multiplier
        self.closed = False

    def post(self, arg):
        return arg * self.multiplier

    def close(self):
        self.closed = True


class BasicApiTest(testify.TestCase):
    def test_no_kwargs(self):
        with vimap.fork(process(), range(10)) as pool:
            res = list(v for _, v in pool.imap([1, 2, 3, 4]).zip_in_out())
        testify.assert_equal(sorted(res), [2, 4, 6, 8])

    def test_conn_basic_kwarg(self):
        with vimap.fork(
            process_with_conn(conn=MockConnection(3)),
            range(10)
        ) as pool:
            res = list(v for _, v in pool.imap([1, 2, 3, 4]).zip_in_out())
        testify.assert_equal(sorted(res), [3, 6, 9, 12])

    def test_conn_forked_kwarg(self):
        with vimap.fork(
            process_with_conn(conn=vimap.post_fork_closable_arg(
                MockConnection,
                call_with_worker_index=True
            )),
            [1, 2000]
        ) as pool:
            res = list(v for _, v in pool.imap(xrange(1, 1000)).zip_in_out())

        # for 1000 inputs, we assume some of them are going to
        # to go each of our two workers.
        assert len([x for x in res if x > 2000]) > 1
        assert len([x for x in res if x < 2000]) > 1
