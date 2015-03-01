"""
Unit tests for the queue manager.
"""
from __future__ import absolute_import
from __future__ import print_function

import multiprocessing

import testify

import vimap.config
import vimap.queue_manager


class FullInputQueue(object):
    def put(self, x, timeout=None):
        testify.assert_is_not(timeout, None)
        raise multiprocessing.queues.Full()


class AcceptingInputQueue(object):
    def put(self, x, timeout=None):
        testify.assert_is_not(timeout, None)


class AcceptingOneInputQueue(object):
    def __init__(self):
        self.accepted = False

    def put(self, x, timeout=None):
        testify.assert_is_not(timeout, None)
        if self.accepted:
            raise multiprocessing.queues.Full()
        else:
            self.accepted = True
            self.put_value = x


class EmptyOutputQueue(object):
    def get_nowait(self):
        raise multiprocessing.queues.Empty()


class RandomTimeoutOutputQueue(object):
    def __init__(self, lst):
        self.lst = lst
        self.timeout_every_other = True

    def get_nowait(self):
        self.timeout_every_other = not self.timeout_every_other
        if self.timeout_every_other or not self.lst:
            raise multiprocessing.queues.Empty()
        else:
            return self.lst.pop(0)


def get_qm(
    input_queue,
    output_queue,
    max_real_in_flight=10,
    max_total_in_flight=20,
    timeouts_config=vimap.config.TimeoutConfig.default_config()
):
    qm = vimap.queue_manager.VimapQueueManager(
        max_real_in_flight=max_real_in_flight,
        max_total_in_flight=max_total_in_flight,
        timeouts_config=timeouts_config,
    )
    qm.input_queue = input_queue
    qm.output_queue = output_queue
    return qm


class QueueManagerTest(testify.TestCase):
    def test_input_queue_full(self):
        qm = get_qm(FullInputQueue(), EmptyOutputQueue())
        qm.spool_input(iter([1, 2, 3]))
        testify.assert_equal(qm.tmp_input_queue, [1, 2, 3])
        testify.assert_equal(qm.tmp_output_queue, [])
        testify.assert_equal(qm.num_real_in_flight, 0)
        testify.assert_equal(qm.num_total_in_flight, 3)

    def test_input_queue_accepting_one(self):
        in_queue = AcceptingOneInputQueue()
        qm = get_qm(in_queue, EmptyOutputQueue())
        qm.spool_input(iter([1, 2, 3]))
        testify.assert_equal(qm.tmp_input_queue, [2, 3])
        testify.assert_equal(in_queue.put_value, 1)
        testify.assert_equal(qm.tmp_output_queue, [])
        testify.assert_equal(qm.num_real_in_flight, 1)
        testify.assert_equal(qm.num_total_in_flight, 3)

    def test_random_timeout_output_queue(self):
        qm = get_qm(AcceptingInputQueue(), RandomTimeoutOutputQueue([3, 4]))
        qm.spool_input(iter([1, 2]))
        for _ in xrange(4):
            qm.feed_out_to_tmp()
        testify.assert_equal(qm.tmp_input_queue, [])
        testify.assert_equal(qm.tmp_output_queue, [3, 4])
        testify.assert_equal(qm.num_real_in_flight, 0)
        testify.assert_equal(qm.num_total_in_flight, 2)
