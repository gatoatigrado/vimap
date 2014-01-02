# -*- coding: utf-8 -*-
'''
Allows one to print the state of all processes. Each process's state is written in code,

 * '.' = initializing
 * 'i' = waiting on input queue
 * 'o' = waiting on output queue
 * 'R' = running
 * 'X' = died from exception
 * '$' = finished

Then, when you see a string like 'RiiRf', you can easily tell two processes are running,
two are waiting for input, and one is finished. Unlike numerical values, these strings
will also let you know if the same thread is running/blocked/etc., which can be helpful
for diagnosing performance problems.
'''
from __future__ import absolute_import
from __future__ import print_function

import multiprocessing


class PoolSharedState(object):
    def __init__(self, num_workers):
        self.worker_state = multiprocessing.RawArray('c', num_workers)
        self.worker_state.value = ('^' * num_workers)
        self.num_workers = num_workers

    def state_setter(self, worker_index):
        assert 0 <= worker_index < self.num_workers
        def inner(value):
            try:
                self.worker_state[worker_index] = value
            except: pass
        return inner

    def get_state_string(self):
        return self.worker_state.value[:self.num_workers]
