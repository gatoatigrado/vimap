# -*- coding: utf-8 -*-
"""
Tests that when workers throw exceptions, they're caught and/or printed in
sensible places.
"""
import testify as T

import vimap.pool
import vimap.worker_process


basic_worker = vimap.worker_process.worker(lambda inputs: inputs)


class ChunkedAPITest(T.TestCase):
    def get_chunked_pool(self, default_chunk_size):
        return vimap.pool.fork_chunked(
            [basic_worker.init_args()],
            default_chunk_size=default_chunk_size)

    def test_invalid_chunk_size(self):
        with T.assert_raises(ValueError):
            self.get_chunked_pool(None)

    def test_valid_chunk_size(self):
        self.get_chunked_pool(10)

    def test_floats_are_invalid_chunk_sizes(self):
        with T.assert_raises(ValueError):
            self.get_chunked_pool(10).imap([1, 2, 3], chunk_size=1.0)

    def test_zero_is_an_invalid_chunk_size(self):
        with T.assert_raises(ValueError):
            self.get_chunked_pool(10).imap([1, 2, 3], chunk_size=0)
