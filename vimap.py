'''
Like multiprocessing.imap_unordered(), but more flexible. In particular, it
allows,

 * Worker processes to initialize connections, etc. on startup
 * Initialization arguments have a process ID
 * Exceptions to be passed through to the main class

The main declaration of worker routines is to give a function, which first
takes an input sequence, then any other options.

    @imap2.worker
    def worker(input_sequence, server=None):
        # Sometimes you want stuff
        conn = open_worker_connection(server)

        for item in input_sequence:
            x = do_work(conn, item)
            yield f(x)

        conn.close()

In order to provide for useful initialization, that's also per-worker, we
explicitly declare a list of processes,

    processes = imap2.pool(worker.init_args(server=server) for server in servers)

For simple tasks, we probably just want to iterate over input-output pairs,

    for input, output in processes.imap(entire_input_sequence).zip_in_out():
        print("For input {0}, got result {1}".format(input, output))

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
from __future__ import print_function
import itertools
import multiprocessing
import multiprocessing.queues
import sys
from collections import namedtuple


_IDLE_TIMEOUT = 0.02


_Imap2BoundWorker = namedtuple("_Imap2BoundWorker", ["fcn", "args", "kwargs"])


class Imap2WorkerWrapper(object):
    '''Thing returned from imap2.worker'''
    def __init__(self, fcn):
        self.fcn = fcn

    def init_args(self, *args, **kwargs):
        return _Imap2BoundWorker(self.fcn, args, kwargs)

worker = Imap2WorkerWrapper


def child_routine(fcn):
    def real_worker_routine(init_args, init_kwargs, input_queue, output_queue):
        '''
        Takes ordered items from input_queue, lets `fcn` iterate over
        those, and puts items yielded by `fcn` onto the output queue,
        with their IDs.
        '''
        i = [None] # just a mutable value
        def queue_generator():
            while True:
                try:
                    x = input_queue.get(timeout=_IDLE_TIMEOUT)
                    # print("Got {0} from input queue.".format(x))
                    if x is None:
                        return
                    i[0], z = x
                    yield z
                except multiprocessing.queues.Empty:
                    # print("Waiting")
                    pass
                except IOError as e:
                    print("Worker error getting item from input queue",
                        file=sys.stderr)
                    raise
        try:
            for output in fcn(queue_generator(), *init_args, **init_kwargs):
                assert i is not None, ("Produced output before getting first "
                    "input, or multiple outputs for one input.")
                output_queue.put( (i[0], 'output', output) )
                i[0] = None
        except Exception as e:
            output_queue.put( (i[0], 'exception', e) )

    return real_worker_routine


class Imap2Pool(object):
    '''Args: Sequence of imap2 workers.'''

    def __init__(self, worker_sequence):
        self.input_queue = multiprocessing.Queue()
        self.output_queue = multiprocessing.Queue()

        worker_sequence = list(worker_sequence)

        self.processes = []
        for worker in worker_sequence:
            process = multiprocessing.Process(
                target=child_routine(worker.fcn),
                args=(worker.args, worker.kwargs, self.input_queue, self.output_queue))
            process.start()
            self.processes.append(process)

        self.input_uid_ctr = 0
        self.input_uid_to_input = {} # input to keep around until handled
        self.input_sequences = []
        self.finished_workers = False

    def finish_workers(self):
        '''Sends stop tokens to subprocesses, then joins them.'''
        if not self.finished_workers:
            self.finished_workers = True
            for _ in self.processes:
                self.input_queue.put(None)
            for process in self.processes:
                process.join()

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

    def map(self, *args, **kwargs):
        '''Like `imap`, but adds the entire input sequence.'''
        self.imap(*args, **kwargs).enqueue_all()
        return self

    @property
    def all_input(self):
        '''Input from all calls to imap; downside of this approach
        is that it keeps around dead iterators.
        '''
        return (x for seq in self.input_sequences for x in seq)

    def spool_input(self, close_if_done=True):
        '''Put input on the queue. Spools enough input for twice the
        number of processes.
        '''
        n_to_put = 2 * len(self.processes) - self.input_queue.qsize()
        if n_to_put > 0:
            inputs = list(itertools.islice(self.all_input, n_to_put))
            for x in inputs:
                self.enqueue(x)
            if close_if_done and (not inputs):
                self.finish_workers()

    def enqueue(self, (x, xser)):
        '''
        Arguments:
            x -- the real input element
            xser -- the input element to be serialized and sent
                to the worker process
        '''
        uid = self.input_uid_ctr
        self.input_uid_ctr += 1
        self.input_uid_to_input[uid] = x

        try:
            self.input_queue.put((uid, xser))
        except IOError as e:
            print("Error enqueueing item from main process", file=sys.stderr)
            raise

    def enqueue_all(self):
        '''Enqueue all input sequences assigned to this pool.'''
        for x in self.all_input:
            self.enqueue(x)
    # ------

    # === Results-consuming functions
    def zip_in_out(self, close_if_done=True):
        def has_output_or_inflight():
            '''returns True if there are processes alive, or items on the
            output queue.
            '''
            return (not self.output_queue.empty()) or (
                (sum(p.is_alive() for p in self.processes) > 0))

        self.spool_input(close_if_done=close_if_done)

        while self.input_uid_to_input and has_output_or_inflight():
            self.spool_input(close_if_done=close_if_done)
            try:
                uid, typ, output = self.output_queue.get(timeout=0.1)
                if typ == 'output':
                    yield self.input_uid_to_input.pop(uid), output
                elif typ == 'exception':
                    print("zip_in_out: Worker exception {0}".format(output),
                        file=sys.stderr)
            except multiprocessing.queues.Empty:
                pass
            except IOError as e:
                print("Error getting output queue item from main process",
                    file=sys.stderr)
                raise
        if close_if_done:
            self.finish_workers()
        # Return when input given is exhausted, or workers die from exceptions
    # ------

pool = Imap2Pool


def unlabeled_pool(worker_fcn, *args, **kwargs):
    '''Shortcut for when you don't care about per-worker initialization
    arguments.

    Example usage:

        parse_mykey = imap2.unlabeled_pool(
            lambda line: simplejson.loads(line)['mykey'])
        entries = parse_mykey.imap(fileinput.input())
    '''
    num_workers = kwargs.pop('num_workers', None)
    if num_workers is None:
        num_workers = multiprocesing.cpu_count()
    return pool(worker_fcn.init_args(*args, **kwargs)
        for _ in range(num_workers))


if __name__ == "__main__":
    import time
    @worker
    def worker_proc(seq, init=0):
        print("got init arg {0}".format(init))
        for x in seq:
            time.sleep(0.1)
            yield x + init

    processes = pool(worker_proc.init_args(init=i) for i in [1, 2, 3])
    print(list(processes.zip_in_out()))

    processes = pool(worker_proc.init_args(init=i) for i in [1, 2, 3])

    results = list(processes.imap([4, 4, 4]).zip_in_out(close_if_done=False))
    print(results)
    assert set(results) == set([(4, 5), (4, 6), (4, 7)])

    results = list(processes.imap([4, 4, 4]).zip_in_out(close_if_done=False))
    print(results)
    assert set(results) == set([(4, 5), (4, 6), (4, 7)])

    for input, output in processes.imap([4, 4, 4] * 3).zip_in_out():
        print("For input {0} got result {1}".format(input, output))
        time.sleep(0.3)
