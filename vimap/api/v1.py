"""
Draft v1 API.

Issues I'm trying to fix in this API:

 * No more forgetting to `yield` for every input consumed
 * Reconcile differences between `fork` and `fork_identical`
 * Fix broken kwarg interface in `fork_identical` (some kwargs
   get passed to the workers, others get used by the pool
   depending on their names ... it's not thaaat terrible in
   practice but can be better)

and, in general, of course just trying to make common cases
easier.
"""
import abc
import contextlib
import itertools
from collections import namedtuple

__author__ = 'gatoatigrado'

from __future__ import absolute_import
from __future__ import print_function

import functools


class Worker(object):
    __metaclass__ = abc.ABCMeta

    def post_fork_contextmanager(self, worker_index):
        yield

    @abc.abstractmethod
    def process(self, item):
        pass

    def _as_old_worker(self):
        import vimap.worker_process

        @vimap.worker_process
        def old_worker_process(inputs, worker_index):
            with self.post_fork_contextmanager(worker_index):
                for x in inputs:
                    yield self.process(x)
        return old_worker_process


# `newtype` equivalent of @contextlib.contextmanager
#
# PostForkContextManagers are arg values that get initialized
# per worker. They're meant for the common case of opening
# connections, etc. on worker processes.
#
# Each argument to PostForkContextManager is the same as what's
# passed to an @contextlib.contextmanager, except that
PostForkContextManager = namedtuple("PostForkContextManager", ["inner_fcn"])


def split_to_dict(data, key):
    """Splits data by `key`. Example

    data = [1, 2, 3, 4]
    key = lambda x: x % 2 == 0

    The result would be

    {
        True: (2, 4),  # these are even / `key` returns True
        False: (1, 3)
    }
    """
    by_bins_iter = itertools.groupby(sorted(data, key=pred), key=pred)
    return dict((k, tuple(v)) for k, v in by_bins_iter)


def split_to_fixed_bins(data, key, bins):
    dct = split_to_dict(data, key)
    return tuple(dct.get(bin, ()) for bin in bins)


class SmartKwargsWorker(Worker):
    """
    Does a little "magic" (i.e. type branching) on kwargs for convenience.
    """
    def __init__(self, **kwargs):
        self._prefork_kwargs = kwargs

        # stuff will be unhappy if these get overridden
        FORBIDDEN_NAMES = ('shutdown_cleanup', 'process')
        assert not any(
            key in FORBIDDEN_NAMES
            for key in kwargs.keys()
        )

    def post_fork_initialize(self, worker_index):
        normal, postfork = split_to_fixed_bins(
            self._prefork_kwargs.items(),
            (lambda key, x: (
                'postfork'
                if isinstance(x, PostForkContextManager)
                else 'normal'
            )),
            ('normal', 'postfork')
        )

        # set all non-special variables on `self`
        vars(self).update(dict(normal))

        # set all other variables
        with contextlib.nested(*[v for k, v in postfork]) as values:
            vars(self.update(
                dict(zip([k for k, v in postfork], values))
            ))
            yield


class WorkerFromFcn(SmartKwargsWorker):
    def __init__(self, worker_method, **kwargs):
        self.worker_method = worker_method
        super(WorkerFromFcn, self).__init__(**kwargs)

    def process(self, item):
        non_fragile_vars = dict(
            (k, vars(self)[k])
            for k in self._prefork_kwargs.keys()
        )
        return self.worker_method(item, **non_fragile_vars)


def post_fork_closable_arg(opener, call_with_worker_index=False):
    """
    Common case for things like connection openers.

    :param opener:
        A 0 or 1-argument function that returns a closable object
    :type opener:
        () -> Closable a, or
        (worker_index,) -> Closable a
    :param call_with_worker_index:
        Whether `opener` is a 1-argument function, to be called
        with the worker index, or not
    :type call_with_worker_index: bool
    :return:
        A PostForkContextManager thing, suitable for passing into
        SmartKwargsWorker classes.
    """
    def contextmanager_fcn(worker_index):
        closable = (
            opener(worker_index)
            if call_with_worker_index
            else opener()
        )
        try:
            yield closable
        finally:
            closable.close()
    return PostForkContextManager(contextmanager_fcn)


# convenience decorator if you like to write your worker
# methods at global scope.
fcn_worker = lambda fcn: functools.partial(WorkerFromFcn, fcn)


def fork(worker, worker_indices):
    pass
