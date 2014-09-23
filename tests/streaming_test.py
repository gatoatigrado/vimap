# -*- coding: utf-8 -*-
"""
Tests that vimap streams data lazily.
"""
from __future__ import absolute_import
from __future__ import print_function

import weakref

import testify as T

import vimap.pool
import vimap.worker_process


@vimap.worker_process.worker
def do_nothing_worker(inputs):
    return inputs


class StreamingTest(T.TestCase):
    def fork_pool(self):
        """Overridden by the StrictInflightStreamingTest."""
        return vimap.pool.fork_identical(do_nothing_worker, num_workers=1)

    def test_streaming(self):
        """Cleverish test to check that vimap is really streaming. Essentially
        we make the input generator that emits,

            [0, 1, 2, 3, ..., 99]  # variable inputs_which_must_be_processed

        and then emits [None, None, ...] until each of the numerical inputs
        have been processed (fed through the worker, and retrieved as output).
        """
        inputs_which_must_be_processed = frozenset(xrange(100))
        already_processed = set()
        num_elements_total = 0

        def input_generator():
            for i in sorted(inputs_which_must_be_processed):
                yield i
            while not already_processed.issuperset(inputs_which_must_be_processed):
                yield None

        pool = self.fork_pool()
        for in_, _ in pool.imap(input_generator()).zip_in_out():
            already_processed.add(in_)
            num_elements_total += 1

        # NOTE: streaming_lookahead is the number of None elements emitted by
        # input_generator(). It can be greater than zero, when the worker
        # hasn't finished processing the first 100 numerical inputs, but our
        # main thread wants to enqueue more inputs (to keep the workers busy).
        streaming_lookahead = num_elements_total - len(inputs_which_must_be_processed)
        T.assert_gte(
            streaming_lookahead,
            0,
            "Sanity check failed.")

        # Note: This can *very* occasionally flake, since we can feed a bunch
        # of stuff to the input queue, pull a bunch to the temporary output
        # buffer (in the queue manager), but only yield one element from the
        # zip_in_out() function.
        #
        # We may refine streaming properties to make this impossible, but in
        # general vimap works under the assumption that the input may be an
        # infinte stream, but should be something we can do some limited
        # non-blocking read-ahead with.
        T.assert_lte(
            streaming_lookahead,
            pool.qm.max_total_in_flight,
            "max_total_in_flight is a hard upper bound, but was violated.")


class StrictInflightStreamingTest(StreamingTest):
    """Checks that max_total_in_flight acts as a hard upper bound on the
    number of inputs spooled.
    """
    def fork_pool(self):
        pool = vimap.pool.fork([do_nothing_worker.init_args()], max_total_in_flight_factor=2)

        # This assert checks that the max_total_in_flight argument is properly
        # propagated to the queue manager, and makes it directly obvious that
        # the last assert in test_streaming does something
        # (the assert "streaming_lookahead <= pool.qm.max_total_in_flight")
        assert pool.qm.max_total_in_flight == 2

        return pool


class MyFancyClass(object):
    def __init__(self, x):
        self.x = x


class InputRefsReleasedTest(T.TestCase):
    """Checks that references to input elements are released."""
    def test(self):
        weakrefs = []
        inputs = (MyFancyClass(x) for x in xrange(100))

        def inputs_with_weakrefs():
            for x in inputs:
                weakrefs.append(weakref.ref(x))
                yield x

        pool = vimap.pool.fork_identical(do_nothing_worker, num_workers=1)
        pool.imap(inputs_with_weakrefs()).block_ignore_output()
        assert all(wr() is None for wr in weakrefs)
