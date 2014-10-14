# -*- coding: utf-8 -*-
'''
Provides process pools for vimap.

TBD:

For more complex tasks, which we might want to handle exceptions,

    def process_result(input):
        try:
            result = (yield)
            print("For input {0} got result {1}".format(input, result)
        except Exception as e:
            print("While processing input {0}, got exception {1}".format(input, e))

    processes.imap(entire_input_sequence).handle_result(process_result)

You can also use it in a more "async" manner, e.g. when your input sequences are
relatively small and/or calculated ahead of time, you can write,

    processes.map(seq1)
    processes.map(seq2)

(by default, input is only enqueued as results are consumed.)
'''
from __future__ import absolute_import
from __future__ import print_function

import multiprocessing
import multiprocessing.queues
import sys
import time
import weakref

import vimap.config
import vimap.exception_handling
import vimap.queue_manager
import vimap.real_worker_routine
import vimap.chunked_real_worker_routine
import vimap.util


NO_INPUT = 'NO_INPUT'


_DEFAULT_DEFAULT_CHUNK_SIZE = 100


class VimapPool(object):
    '''Args: Sequence of vimap workers.'''

    process_class = multiprocessing.Process
    worker_routine_class = vimap.real_worker_routine.WorkerRoutine
    queue_manager_class = vimap.queue_manager.VimapQueueManager

    # TODO: Implement timeout in joining workers
    #
    def __init__(
            self,
            worker_sequence,
            max_real_in_flight_factor=10,
            max_total_in_flight_factor=100,
            timeouts_config=vimap.config.TimeoutConfig(5.0),
            debug=False
    ):
        """
        Get a new vimap pool

        :param worker_sequence: Sequence of workers
        :type worker_sequence:
        :param max_real_in_flight_factor:
            How much data to spool.
        :type max_real_in_flight_factor: int
        :param max_total_in_flight_factor:
            How much data to buffer in in-memory queues,
            as well as actual FIFO queues.
        :type max_total_in_flight_factor: int
        :param timeouts_config: Configuration related to timeouts_config
        :type timeouts_config: vimap.config.TimeoutConfig
        :param debug: print debugging information
        :type debug: bool
        """
        self.max_real_in_flight_factor = max_real_in_flight_factor
        self.worker_sequence = list(worker_sequence)

        self.qm = self.queue_manager_class(
            max_real_in_flight=self.max_real_in_flight_factor * len(self.worker_sequence),
            max_total_in_flight=max_total_in_flight_factor * len(self.worker_sequence),
            timeouts_config=timeouts_config,
            debug=debug)

        # Don't prevent `self` from being GC'd
        self_ref = weakref.ref(self)

        def check_output_for_error(item):
            uid, typ, output = item
            if typ == 'exception':
                vimap.exception_handling.print_exception(output, None, None)
                if self_ref():
                    self_ref().has_exceptions = True
        self.qm.add_output_hook(check_output_for_error)

        self.processes = []

        self.timeouts_config = timeouts_config

        self.input_uid_ctr = 0
        self.input_uid_to_input = {}  # input to keep around until handled
        self.input_sequences = []
        self.has_exceptions = False  # Have any workers thrown exceptions yet?
        self.debug = debug

    num_in_flight = property(lambda self: self.qm.num_total_in_flight)

    _default_print_fcn = lambda msg: print(msg, file=sys.stderr)

    def add_progress_notification(
            self,
            print_interval_s=1,
            item_type="items",
            print_fcn=_default_print_fcn):

        state = {'last_printed': time.time(), 'output_counter': 0}

        def print_output_progress(item):
            state['output_counter'] += 1
            if time.time() - state['last_printed'] > print_interval_s:
                state['last_printed'] = time.time()
                print_fcn("Processed {0} {1}".format(state['output_counter'], item_type))
        self.qm.add_output_hook(print_output_progress)
        return self

    def fork(self, debug=None):
        debug = self.debug if debug is None else debug
        for i, worker in enumerate(self.worker_sequence):
            routine = self.worker_routine_class(
                worker.fcn, worker.args, worker.kwargs, index=i, debug=debug)
            process = self.process_class(
                target=routine.run,
                args=(self.qm.input_queue, self.qm.output_queue))
            process.daemon = True  # processes will be controlled by parent
            assert not process.is_alive()
            process.start()
            assert process.is_alive(), "should be marked alive after starting"
            self.processes.append(process)
        return self

    def __del__(self):
        '''Don't hang if all references to the pool are lost.'''
        self.finish_workers()
        if self.input_uid_to_input and not self.has_exceptions:
            vimap.exception_handling.print_warning(
                "Pool disposed before input was consumed, but no worker "
                "exceptions were caught (or only seen when the pool was "
                "deleted)")

    def all_processes_died(self, exception_check_optimization=True):
        if exception_check_optimization and (not self.has_exceptions):
            return False
        return not any(p.is_alive() for p in self.processes)

    @vimap.util.instancemethod_runonce()
    def send_stop_tokens(self):
        '''Sends stop tokens to the worker processes, telling them to shut
        down. Note that normal inputs are of the form (idx, value), whereas
        the stop token is not a tuple, so inputs can't be mistaken for stop
        tokens and vice-versa.
        '''
        # FIXME(gatoatigrado|2014-07-31): For some reason, sending stop
        # tokens for only alive processes fails.
        self.qm.send_stop_tokens(len(self.processes))

    @vimap.util.instancemethod_runonce(depends=['send_stop_tokens'])
    def join_and_consume_output(self):
        # This will feed items from the output queue until it's
        # empty. However, we need to keep spooling from the output
        # queue as processes die, or else other processes may not
        # be able to enqueue their final items to the output queue
        # (since it's full).
        while not self.all_processes_died(exception_check_optimization=False):
            self.qm.feed_out_to_tmp(max_time_s=None)
            time.sleep(0.001)
        self.qm.feed_out_to_tmp(max_time_s=None)

        for process in self.processes:
            process.join()
        # NOTE: Not only prevents future erroneous accesses, 'del' is actually
        # necessary to clean up / close the pipes used by the process.
        del self.processes

        self.qm.close()

    @vimap.util.instancemethod_runonce()
    def finish_workers(self):
        '''Sends stop tokens to subprocesses, then joins them. There may still be
        unconsumed output.

        This method is called when you call zip_in_out() with finish_workers=True
        (the default), as well as when the GC reclaims the pool.
        '''
        if self.debug:
            print("Main thread: Finishing workers")
        self.send_stop_tokens()
        self.join_and_consume_output()

    # === Input-enqueueing functionality
    def imap(self, input_sequence, pretransform=False):
        '''Spools bits of an input sequence to workers' queues; good
        for doing things like iterating through large files, live
        inputs, etc. Otherwise, use map.

        Keyword arguments:
            pretransform -- if True, then assume input_sequence items
                are pairs (x, tf(x)), where tf is some kind of
                pre-serialization transform, applied to input elements
                before they are sent to worker processes.
        '''
        if pretransform:
            self.input_sequences.append(iter(input_sequence))
        else:
            self.input_sequences.append(((v, v) for v in input_sequence))
        self.spool_input(close_if_done=False)
        return self

    # NOTE: `map` may overwhelm the output queue and cause things to freeze,
    # therefore it's getting removed for now. Plans to re-add it are not
    # imminent.

    @property
    def all_input_serialized(self):
        '''Input from all calls to imap; downside of this approach
        is that it keeps around dead iterators.
        '''
        def get_serialized((x, xser)):
            uid = self.input_uid_ctr
            self.input_uid_ctr += 1
            self.input_uid_to_input[uid] = x
            return (uid, xser)
        return (get_serialized(x) for seq in self.input_sequences for x in seq)

    def spool_input(self, close_if_done=False):
        '''Put input on the queue. If `close_if_done` and we reach the end
        of the input stream, send stop tokens.
        '''
        if self.qm.spool_input(self.all_input_serialized) and close_if_done:
            # reached the end of the stream
            self.send_stop_tokens()
    # ------

    def get_corresponding_input(self, uid, output):
        '''Find the input object given the output.

        Sometimes we get an exception as output before any input has
        been processed, thus we have no corresponding input.
        '''
        return self.input_uid_to_input.pop(uid, NO_INPUT)

    # === Results-consuming functions
    def zip_in_out_typ(self, close_if_done=True):
        '''Yield (input, output, type) tuples for each input item processed.

        type can either be 'output' or 'exception' and output will
        contain either the output value or the exception, respectively.
        '''
        self.spool_input()
        while self.qm.num_total_in_flight > 0:
            try:
                uid, typ, output = self.qm.pop_output()

                # Spool more so we don't exit prematurely
                self.spool_input(close_if_done=close_if_done)

                inp = self.get_corresponding_input(uid, output)
                yield inp, output, typ
            except multiprocessing.queues.Empty:
                # If processes are still running, then just wait for
                # more output. If not, we've exhausted the ouput and
                # break.
                if self.all_processes_died():
                    # num_total_in_flight is messed up (will always be
                    # positive). We must exit.
                    vimap.exception_handling.print_warning(
                        "All processes died prematurely!")
                    break
                time.sleep(0.01)
            except IOError:
                print(
                    "Error getting output queue item from main process",
                    file=sys.stderr)
                raise
        if close_if_done:
            self.finish_workers()
        # Return when input given is exhausted, or workers die from exceptions

    def zip_in_out(self, *args, **kwargs):
        '''Yield (input, output) tuples for each input item processed
        skipping inputs that had an exception.
        '''
        for inp, output, typ in self.zip_in_out_typ(*args, **kwargs):
            if typ == 'output':
                yield inp, output
    # ------

    def block_ignore_output(self, *args, **kwargs):
        for _ in self.zip_in_out(*args, **kwargs):
            pass


class ChunkedPool(VimapPool):
    worker_routine_class = vimap.chunked_real_worker_routine.ChunkedWorkerRoutine

    def __init__(self, *args, **kwargs):
        self.default_chunk_size = kwargs.pop(
            'default_chunk_size',
            _DEFAULT_DEFAULT_CHUNK_SIZE)
        super(ChunkedPool, self).__init__(*args, **kwargs)
        self.check_chunk_size(self.default_chunk_size)

    def check_chunk_size(self, chunk_size):
        if not (isinstance(chunk_size, int) and chunk_size > 0):
            raise ValueError("Invalid chunk size {0}!".format(chunk_size))

    # Standard vimap API
    def imap(self, input_sequence, chunk_size=None):
        """By default, using the regular vimap API (in this case, imap) will
        automatically chunk input.
        """
        chunk_size = (self.default_chunk_size if chunk_size is None else chunk_size)
        self.check_chunk_size(chunk_size)
        return self.imap_chunks(vimap.util.chunk(input_sequence, chunk_size))

    def zip_in_out_typ(self, *args, **kwargs):
        """By default, the regular vimap API will un-chunk output. So, as we
        get back output from the base API, we yield an input-output pair
        for everything in the chunk.
        """
        for inp, output, typ in self.zip_in_out_typ_chunks(*args, **kwargs):
            if typ == 'output':
                assert len(inp) == len(output)
                for in_elt, out_elt in zip(inp, output):
                    yield in_elt, out_elt, typ
            else:
                # To have a consistent API, we untuple the input and say the exception
                # happened on the first element (which may not be true); if the
                # input is malformed in any way (or the NO_INPUT token), we
                # pass it through unprocessed.
                inp = (inp[0] if (isinstance(inp, (list, tuple)) and inp) else inp)
                yield inp, output, typ

    # Chunked API -- provides a lower-level API to directly enqueue or consume
    # chunks.
    def imap_chunks(self, *args, **kwargs):
        """We rename the base pool's `imap` to `imap_chunks`, so it's obvious
        the caller is putting in chunks, which will be un-chunked in the worker
        routine.
        """
        return super(ChunkedPool, self).imap(*args, **kwargs)

    def zip_in_out_typ_chunks(self, *args, **kwargs):
        """Same idea as `imap_chunks`; this exposes the base pool's
        zip_in_out_typ() function, which returns chunks of input associated
        to chunks of output.
        """
        return super(ChunkedPool, self).zip_in_out_typ(*args, **kwargs)

    def zip_in_out_chunks(self, *args, **kwargs):
        """To provide chunked APIs for all methods, we also add a zip_in_out_chunks()
        which is like zip_in_out_typ_chunks but filters on typ == 'output'.
        """
        for inp, output, typ in self.zip_in_out_typ_chunks(*args, **kwargs):
            if typ == 'output':
                yield inp, output


def fork(*args, **kwargs):
    return VimapPool(*args, **kwargs).fork()


def fork_chunked(*args, **kwargs):
    return ChunkedPool(*args, **kwargs).fork()


def _fork_identical_base(fork_method, worker_fcn, *args, **kwargs):
    '''Shortcut for when you don't care about per-worker initialization
    arguments.
    '''
    num_workers = kwargs.pop('num_workers', multiprocessing.cpu_count())
    return fork_method(worker_fcn.init_args(*args, **kwargs) for _ in range(num_workers))


def fork_identical(*args, **kwargs):
    return _fork_identical_base(fork, *args, **kwargs)


def fork_identical_chunked(*args, **kwargs):
    return _fork_identical_base(fork_chunked, *args, **kwargs)
