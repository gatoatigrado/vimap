# -*- coding: utf-8 -*-
'''
Tests peformance in a system-agnostic manner, by comparing single-threaded
performance with parallel performance.
'''
from __future__ import absolute_import
from __future__ import print_function

import multiprocessing
import time
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


@vimap.worker_process.worker
def simple_sleep_worker(sleep_times):
    for time_to_sleep in sleep_times:
        time.sleep(time_to_sleep)
        yield None


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

    def run_big_fork_test(self, time_sleep_s, num_workers, num_inputs, num_test_iterations):
        """Common setup for the big fork test; see usage in the two tests below.

        :returns: The amount of time the test took, in seconds.
        """
        pool = vimap.pool.fork_identical(simple_sleep_worker, num_workers=num_workers)

        def sleep_in_parallel():
            pool.imap(time_sleep_s for _ in xrange(num_inputs))
            pool.block_ignore_output(close_if_done=False)

        return timeit.timeit(sleep_in_parallel, number=num_test_iterations) / num_test_iterations

    def test_big_fork(self):
        """Tests that we can fork a large number of processes, each of which
        will wait for a few milliseconds, and return.

        NOTE: currently fails if you bump 70 up to 200. We're going to fix this very soon.
        """
        time_sleep_s = 0.2
        test_time = self.run_big_fork_test(time_sleep_s, 70, 70, 3)
        print("Big fork performance test: {0:.2f} s (nominal: {1:.2f} s)".format(
            test_time, time_sleep_s))
        T.assert_lt(test_time, time_sleep_s * 2)

    def test_big_fork_test(self):
        """Tests that if we have one more input, the big fork performance test
        would fail. This makes sure the above test is really doing something.
        """
        time_sleep_s = 0.2
        test_time = self.run_big_fork_test(time_sleep_s, 70, 71, 1)
        T.assert_gt(test_time, time_sleep_s * 2)
