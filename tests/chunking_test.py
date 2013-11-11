# -*- coding: utf-8 -*-
"""
Tests that when workers throw exceptions, they're caught and/or printed in
sensible places.
"""
import math

import testify as T

import vimap.pool
import vimap.worker_process


basic_worker = vimap.worker_process.worker(lambda inputs: inputs)


class ChunkedAPITest(T.TestCase):
    def test_valid_chunk_size(self):
        with T.assert_raises(ValueError):
            vimap.pool.fork_chunked([basic_worker.init_args()], default_chunk_size=None)

        pool = vimap.pool.fork_chunked([basic_worker.init_args()], default_chunk_size=10)

        # Can't be a float
        with T.assert_raises(ValueError):
            pool.imap([1, 2, 3], chunk_size=1.0)

        # Can't be zero
        with T.assert_raises(ValueError):
            pool.imap([1, 2, 3], chunk_size=0)
