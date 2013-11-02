# -*- coding: utf-8 -*-
'''
Provides an interface for defining worker processes.
'''
from __future__ import absolute_import
from __future__ import print_function

import errno
import os
import stat
import vimap.pool
import vimap.worker_process
import testify as T


_MAX_FDS_TO_SCAN = 30  # Number of FDs to scan


# decrypt POSIX stuff
readable_mode_strings = {
    'directory': stat.S_ISDIR,
    'character_device': stat.S_ISCHR,
    'block_device': stat.S_ISBLK,
    'regular': stat.S_ISREG,
    'fifo': stat.S_ISFIFO,
    'symlink': stat.S_ISLNK,
    'socket': stat.S_ISSOCK }


def fd_type_if_open(fd_number):
    """For a given file descriptor, return None if the FD is closed, else
    a list of human-readable strings describing the file type.
    """
    try:
        fd_stat = os.fstat(fd_number)
        modes = [
            k for k, v in readable_mode_strings.items()
            if v(fd_stat.st_mode)]
        return modes
    except OSError as e:
        if e.errno == errno.EBADF:  # "Bad file descriptor"
            return None
        else:
            raise


def get_open_fds():
    fd_stats = [fd_type_if_open(i) for i in xrange(3, _MAX_FDS_TO_SCAN)]
    assert not any(fd_stats[-10:]), "Increase _MAX_FDS_TO_SCAN"
    return fd_stats


def difference_open_fds(before_fds, after_fds):
    # handy zipped list
    lst = zip(xrange(3, _MAX_FDS_TO_SCAN), before_fds, after_fds)
    lst = [(i, before, after) for i, before, after in lst if before or after]
    return {
        'closed': [(i, before) for i, before, after in lst if not after],
        'opened': [(i, after) for i, before, after in lst if not before] }


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
        T.assert_equal(len(difference_open_fds(first, second)['opened']), 2)
        T.assert_equal(len(difference_open_fds(first, second)['closed']), 0)
        T.assert_equal(len(difference_open_fds(second, third)['closed']), 1)
        T.assert_equal(len(difference_open_fds(second, third)['opened']), 0)


@vimap.worker_process.worker
def basic_worker(xs):
    for x in xs:
        yield x + 1


class TestBasicMapDoesntLeaveAroundFDs(T.TestCase):
    def iteration_with_zip_auto_close(self):
        initial_open_fds = get_open_fds()
        pool = vimap.pool.fork_identical(basic_worker, num_workers=1)
        after_fork_open_fds = get_open_fds()
        results = list(pool.imap([1, 2, 3]).zip_in_out())
        after_finish_open_fds = get_open_fds()

        # Check that some FDs were opened after forking
        after_fork = difference_open_fds(initial_open_fds, after_fork_open_fds)
        # T.assert_equal(after_fork['closed'], [])
        T.assert_gte(len(after_fork['opened']), 2)  # should have at least 3 open fds
        # All opened files should be FIFOs
        T.assert_equal(all(typ == ['fifo'] for i, typ in after_fork['opened']), True)

        # FIXME: Some FDs, which don't show up in strace, seem to stick around.
        # I'm not sure why :(.
        after_cleanup = difference_open_fds(after_fork_open_fds, after_finish_open_fds)
        T.assert_gte(len(after_cleanup['closed']), 2)

    def test_with_zip_auto_close(self):
        initial_open_fds = get_open_fds()
        for _ in xrange(10):
            self.iteration_with_zip_auto_close()
        final_open_fds = get_open_fds()

        fds_left_around = difference_open_fds(initial_open_fds, final_open_fds)
        # FIXME: See fixme above. Ideally this would be zero.
        T.assert_lte(len(fds_left_around['opened']), 1)


if __name__ == "__main__":
    T.run()
