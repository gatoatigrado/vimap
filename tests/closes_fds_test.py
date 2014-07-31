# -*- coding: utf-8 -*-
'''
Provides an interface for defining worker processes.
'''
from __future__ import absolute_import
from __future__ import print_function

import errno
import logging
import os
import os.path
import resource
import stat
from collections import namedtuple

import mock
import testify as T

import vimap.pool
import vimap.queue_manager
from vimap.testing import repeat_test_to_catch_flakiness
import vimap.worker_process



# decrypt POSIX stuff
readable_mode_strings = {
    'directory': stat.S_ISDIR,
    'character_device': stat.S_ISCHR,
    'block_device': stat.S_ISBLK,
    'regular': stat.S_ISREG,
    'fifo': stat.S_ISFIFO,
    'symlink': stat.S_ISLNK,
    'socket': stat.S_ISSOCK}


FDInfo = namedtuple("FDInfo", ["modes", "symlink"])
current_proc_fd_dir = lambda *subpaths: os.path.join("/proc", str(os.getpid()), "fd", *subpaths)


def fd_type_if_open(fd_number):
    """For a given open file descriptor, return information about that file descriptor.

    'modes' are a list of human-readable strings describing the file type;
    'symlink' is the target of the file descriptor (often a pipe name)
    """
    fd_stat = os.fstat(fd_number)
    modes = [k for k, v in readable_mode_strings.items() if v(fd_stat.st_mode)]
    if os.path.isdir(current_proc_fd_dir()):
        return FDInfo(
            modes=modes,
            symlink=os.readlink(current_proc_fd_dir(str(fd_number))))
    else:
        return FDInfo(modes=modes, symlink=None)


def list_fds_linux():
    """A method to list open FDs that uses /proc/{pid}/fd."""
    fds = [
        (int(i), current_proc_fd_dir(str(i)))
        for i in os.listdir(current_proc_fd_dir())]
    # NOTE: Sometimes, an FD is used to list the above directory. Hence, we should
    # re-check whether the FD still exists (via os.path.exists)
    return [i for (i, path) in fds if (i >= 3 and os.path.exists(path))]


def list_fds_other():
    """A method to list open FDs that doesn't need /proc/{pid}."""
    max_fds_soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if max_fds_soft == resource.RLIM_INFINITY or not (3 < max_fds_soft < 4096):
        logging.warning(
            "max_fds_soft invalid ({0}), assuming 4096 is a sufficient upper bound"
            .format(max_fds_soft))
        max_fds_soft = 4096

    # The first three FDs are stdin, stdout, and stderr. We're interested in
    # everything after.
    for i in xrange(3, max_fds_soft):
        try:
            os.fstat(i)
            yield i
        except OSError as e:
            if e.errno != errno.EBADF:
                raise


def get_open_fds(retries=3):
    """
    Returns a map,

        fd (int) --> FDInfo
    """
    if os.path.isdir(current_proc_fd_dir()):
        fds = list_fds_linux()
    else:
        fds = list_fds_other()

    try:
        return dict(filter(
            lambda (k, v): v is not None,
            ((i, fd_type_if_open(i)) for i in fds)))
    except OSError:
        if retries == 0:
            raise
        return get_open_fds(retries - 1)


def difference_open_fds(before, after):
    """
    Given two snapshots of open file descriptors, `before` and `after`, returns
    those FDs which were opened (present in `after` but not `before`) and
    closed.
    """
    # "a - b" for dicts -- remove anything in 'a' that has a key in b
    dict_diff = lambda a, b: dict((k, a[k]) for k in (frozenset(a) - frozenset(b)))
    for k in (frozenset(after) & frozenset(before)):
        if before[k] != after[k]:
            print("WARNING: FD {0} changed from {1} to {2}".format(k, before[k], after[k]))
    return {
        'closed': dict_diff(before, after),
        'opened': dict_diff(after, before)}


class TestOpenFdsMethods(T.TestCase):
    """
    Tests that we can detect open file descriptors.
    """
    def test_open_fds(self):
        first = get_open_fds()
        fd = open('vimap/pool.py', 'r')
        fd2 = open('vimap/pool.py', 'r')
        second = get_open_fds()
        fd.close()
        third = get_open_fds()
        fd2.close()
        T.assert_equal(len(difference_open_fds(first, second)['opened']), 2)
        T.assert_equal(len(difference_open_fds(first, second)['closed']), 0)
        T.assert_equal(len(difference_open_fds(second, third)['closed']), 1)
        T.assert_equal(len(difference_open_fds(second, third)['opened']), 0)


@vimap.worker_process.worker
def basic_worker(xs):
    for x in xs:
        yield x + 1


class TestBasicMapDoesntLeaveAroundFDs(T.TestCase):
    @T.setup_teardown
    def instrument_queue_initiation(self):
        old_init = vimap.queue_manager.VimapQueueManager.__init__

        def instrumented_init(*args, **kwargs):
            self.before_queue_manager_init = get_open_fds()
            old_init(*args, **kwargs)
            self.after_queue_manager_init = get_open_fds()
            self.queue_fds = difference_open_fds(
                self.before_queue_manager_init,
                self.after_queue_manager_init)['opened']
        with mock.patch.object(
                vimap.queue_manager.VimapQueueManager,
                '__init__',
                instrumented_init):
            yield

    @repeat_test_to_catch_flakiness(30)
    def test_all_fds_cleaned_up(self):
        initial_open_fds = get_open_fds()
        pool = vimap.pool.fork_identical(basic_worker, num_workers=1)
        after_fork_open_fds = get_open_fds()
        list(pool.imap([1, 2, 3]).zip_in_out())
        after_finish_open_fds = get_open_fds()

        # Check that some FDs were opened after forking
        after_fork = difference_open_fds(initial_open_fds, after_fork_open_fds)
        # T.assert_equal(after_fork['closed'], [])
        T.assert_gte(len(after_fork['opened']), 2)  # should have at least 3 open fds
        # All opened files should be FIFOs
        if not all(info.modes == ['fifo'] for info in after_fork['opened'].values()):
            print("Infos: {0}".format(after_fork['opened']))
            T.assert_not_reached("Some infos are not FIFOs")

        after_cleanup = difference_open_fds(after_fork_open_fds, after_finish_open_fds)
        T.assert_gte(len(after_cleanup['closed']), 2)

        left_around = difference_open_fds(initial_open_fds, after_finish_open_fds)
        if len(left_around['opened']) != 0:
            queue_fds_left_around = dict(
                item for item in self.queue_fds.items() if item[0] in left_around['opened'])
            print("Queue FDs left around: {0}".format(queue_fds_left_around))
        T.assert_equal(len(left_around['opened']), 0)


if __name__ == "__main__":
    T.run()
