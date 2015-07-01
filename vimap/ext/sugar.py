"""
Provides some convenience functions.
"""
import heapq

import vimap.pool
import vimap.util
import vimap.worker_process


def imap_unordered(fcn, iterable, **kwargs):
    """Maps a function over an iterable. Essentially equivalent to
    multiprocessing.Pool().imap_unordered(fcn, iterable). Since vimap
    fixes bugs in multiprocessing, this function can be a nice alternative.

    Keyword arguments:
        num_workers (from fork_identical) -- number of workers
        extra keyword arguments: passed to the function on each iteration
    """
    @vimap.worker_process.worker
    def worker(inputs, **kwargs2):
        for in_ in inputs:
            yield fcn(in_, **kwargs2)
    pool = vimap.pool.fork_identical(worker, **kwargs)
    for _, output in pool.imap(iterable).zip_in_out():
        yield output


def imap_ordered(fcn, iterable, **kwargs):
    """
    Converts unordered imapping into ordered imapping,
    by maintaining a heap of results.
    """
    unordered_results = imap_unordered(
        lambda (i, x): (i, fcn(x)),
        enumerate(iterable),
        **kwargs
    )
    next_index = 0
    heap = []
    for i, fx in unordered_results:
        # optimization: skip heap addition if it's actually next
        if next_index == i:
            next_index += 1
            yield fx
        else:
            heapq.heappush(heap, (i, fx))

        # dequeue while it's good
        while heap and (heap[0][0] == next_index):
            i_next, fx_next = heapq.heappop(heap)
            next_index += 1
            yield fx_next
    assert not heap


def imap_ordered_chunked(fcn, inputs, chunk_size=100, **kwargs):
    """
    Semantically same as imap_ordered. But instead of feeding each single
    input to a process, this method chunks the inputs first and feed each chunk
    of data to a process at a time.

    For example, if input is [1, 2, 3] and chunk_size is 2 the first process
    gets [1, 2] and the second process gets [3].
    """
    inputs_chunked = vimap.util.chunk(inputs, chunk_size)
    output_chunks = imap_ordered(
        lambda chunk, **kwargs2: [fcn(x, **kwargs2) for x in chunk],
        inputs_chunked,
        **kwargs
    )
    return (x for chunk in output_chunks for x in chunk)
