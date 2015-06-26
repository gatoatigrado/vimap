# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import itertools
import multiprocessing

import mock
import testify as T

import vimap.ext.sugar
import vimap.exception_handling


def run_exception_test(imap_ordered_or_unordered):
    """
    Checks that exceptions are re-thrown, for either imap_unordered or imap_ordered.

    :param imap_ordered_or_unordered:
        either vimap.ext.sugar.imap_unordered or ...imap_ordered
    """
    def fcn(x):
        if x:
            raise ValueError("Bad value: {0}".format(x))
        return x

    with mock.patch.object(
        vimap.exception_handling,
        'print_exception',
        autospec=True
    ) as mock_print_exception:
        T.assert_raises_and_contains(
            vimap.exception_handling.WorkerException,
            ("ValueError: Bad value: 3",),
            lambda: tuple(imap_ordered_or_unordered(fcn, [False, 3, 0]))
        )
        T.assert_equal(mock_print_exception.called, True)


class BasicImapUnorderedTest(T.TestCase):
    def test_basic(self):
        def fcn(i, to_add):
            return i + to_add
        T.assert_equal(
            set(vimap.ext.sugar.imap_unordered(fcn, [1, 2, 3], to_add=1)),
            set([2, 3, 4])
        )

    def test_exceptions(self):
        run_exception_test(vimap.ext.sugar.imap_unordered)


class ImapOrderedTests(T.TestCase):
    def test_basic(self):
        for n in [2, 4, 8, 32, 3200, 32000]:
            doubled = tuple(vimap.ext.sugar.imap_ordered(
                lambda x: 2 * x,
                range(n),
                num_workers=8
            ))
            T.assert_equal(doubled, tuple(2 * x for x in range(n)))

    def test_streaming(self):
        input_iter = iter(xrange(int(10000)))
        doubled_stream = vimap.ext.sugar.imap_ordered(
            lambda x: 2 * x,
            input_iter
        )

        # take a few from the doubled output stream
        consumed = tuple(itertools.islice(doubled_stream, 40))

        # exhaust the input
        unspooled_input = tuple(input_iter)

        # now take the rest from the output stream
        rest = tuple(doubled_stream)

        num_processed = len(consumed) + len(rest)

        T.assert_gt(
            len(unspooled_input),
            9000,
            message="Most inputs should not be processed (too much spooling / "
                    "not lazy). Only {0} remained.".format(len(unspooled_input))
        )
        assert num_processed + len(unspooled_input) == 10000, "Something got dropped"

        T.assert_equal(
            consumed + rest,
            tuple(2 * i for i in xrange(num_processed)),
            message="Processed inputs weren't the first in the stream, or are out of order."
        )

    def test_exceptions(self):
        run_exception_test(vimap.ext.sugar.imap_ordered)


class ImapOrderedChunkedTests(T.TestCase):
    def test_basic(self):
        for n in [2, 4, 8, 32, 3200, 32000]:
            doubled = tuple(vimap.ext.sugar.imap_ordered_chunked(
                lambda x: 2 * x,
                range(n),
                num_workers=8
            ))
            T.assert_equal(doubled, tuple(2 * x for x in range(n)))

    def test_chunking(self):
        """
        Makes sure we do chunk the data and each process gets a chunk.
        """
        input_with_pids = tuple(vimap.ext.sugar.imap_ordered_chunked(
            lambda x: (x, multiprocessing.current_process().pid),
            range(8),
            chunk_size=3
        ))

        expected_input_chunks = [(0, 1, 2), (3, 4, 5), (6, 7)]
        actual_input_chunks = []
        for pid, group in itertools.groupby(input_with_pids, key=lambda (x, pid): pid):
            input_chunk, pids = zip(*group)
            actual_input_chunks.append(input_chunk)

        T.assert_equal(expected_input_chunks, actual_input_chunks)

    def test_exceptions(self):
        run_exception_test(vimap.ext.sugar.imap_ordered)
