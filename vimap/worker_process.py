# -*- coding: utf-8 -*-
'''
Provides an interface for defining worker processes.
'''
from __future__ import absolute_import
from __future__ import print_function

import functools
from collections import namedtuple


_Imap2BoundWorker = namedtuple("_Imap2BoundWorker", ["fcn", "args", "kwargs"])


class Imap2WorkerWrapper(object):
    '''Thing returned from imap2.worker'''
    def __init__(self, fcn):
        self.fcn = fcn

    def init_args(self, *args, **kwargs):
        return _Imap2BoundWorker(self.fcn, args, kwargs)


worker = Imap2WorkerWrapper


def instancemethod_worker(fcn):
    return property(lambda self: worker(functools.partial(fcn, self)))
