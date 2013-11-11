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
    def get_speedup_factor(self, baseline_fcn, optimized_fcn, num_tests):
        baseline_performance = timeit.timeit(baseline_fcn, number=num_tests)
        optimized_performance = timeit.timeit(optimized_fcn, number=num_tests)
        _message = "Performance test too fast, susceptible to overhead"
        T.assert_gt(baseline_performance, 0.005, _message)
        T.assert_gt(optimized_performance, 0.005, _message)
        return (baseline_performance / optimized_performance)

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

        speedup_ratio = self.get_speedup_factor(factor_sequential, factor_parallel, 4)
        efficiency = speedup_ratio / num_workers
        print("Easy performance test efficiency: {0:.1f}% ({1:.1f}x speedup)".format(
            efficiency * 100., speedup_ratio))
        T.assert_gt(efficiency, 0.70, "Failed performance test!!")

    def test_chunking_really_is_faster(self):
        inputs = tuple(xrange(10, 100)) * 10
        normal_pool = vimap.pool.fork_identical(factorial_worker, num_workers=2)
        chunked_pool = vimap.pool.fork_identical_chunked(factorial_worker, num_workers=2)

        def factor_normal():
            normal_pool.imap(inputs).block_ignore_output(close_if_done=False)

        def factor_chunked():
            chunked_pool.imap(inputs).block_ignore_output(close_if_done=False)

        speedup_ratio = self.get_speedup_factor(factor_normal, factor_chunked, 2)
        print("Chunked performance test: {0:.1f}x speedup".format(speedup_ratio))
        T.assert_gt(speedup_ratio, 10)
