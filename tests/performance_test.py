'''
Tests peformance in a system-agnostic manner, by comparing single-threaded
performance with parallel performance.
'''

import multiprocessing
import timeit

import testify as T

import vimap.pool
import vimap.worker_process


def factorial(n):
    '''dumb implementation of factorial function'''
    result = 1
    for i in xrange(2, n + 1):
        result *= i
    return result


@vimap.worker_process.worker
def factorial_worker(numbers):
    for number in numbers:
        factorial(number)
        yield None  # in case the numbers are large, ignore communication cost


class PerformanceTest(T.TestCase):
    def test_performance(self):
        # NOTE: Avoid hyperthreading, which doesn't help performance
        # in our test case.
        num_workers = min(8, multiprocessing.cpu_count() / 2)
        T.assert_gt(num_workers, 1, "Too few cores to run performance test.")

        inputs = tuple(xrange(7000, 7100))

        def factor_sequential():
            for i in inputs:
                factorial(i)

        pool = vimap.pool.fork_identical(factorial_worker, num_workers=num_workers)

        def factor_parallel():
            pool.imap(inputs).block_ignore_output(close_if_done=False)

        sequential_performance = timeit.timeit(factor_sequential, number=4)
        parallel_performance = timeit.timeit(factor_parallel, number=4)
        speedup_ratio = sequential_performance / parallel_performance
        linear_speedup_ratio = float(num_workers)
        efficiency = speedup_ratio / linear_speedup_ratio
        print("Easy performance test efficiency: {0:.1f}% ({1:.1f}x speedup)".format(
            efficiency * 100., speedup_ratio))
        T.assert_gt(efficiency, 0.70, "Failed performance test!!")
