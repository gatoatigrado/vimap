"""
Provides some convenience functions.
"""
import vimap.pool
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
