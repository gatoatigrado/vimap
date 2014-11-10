[![Build Status](https://travis-ci.org/gatoatigrado/vimap.png)](https://travis-ci.org/gatoatigrado/vimap)

Variations on imap, not in C
============================

The `vimap` package is designed to provide a more flexible alternative for
`multiprocessing.imap_unordered`. See the “Why Vimap?” section below for more
reasons behind writing this package.

Basic usage
-----------

This is a contrived example, since the communication is more expensive than
computation, but let’s say you want to double a bunch of numbers in parallel.
Your Hello World in Parallel could be,

```python
import vimap.api.v1 as vimap

@vimap.fcn_worker
def double_it(x):
    return x * 2

inputs = (1, 2, 3, 4)
with vimap.fork(double_it()) as pool:
    outputs = tuple(pool.imap(inputs))

print outputs  # returns doubled numbers in arbitrary order, like (6, 8, 2, 4)
```

Okay, great. But you really want to do some expensive / time-consuming work.
Maybe making a bunch of HTTP requests, and processing the output.

```python
import requests
# NOTE: in the future, this will be just `import vimap`
import vimap.api.v1 as vimap

@vimap.fcn_worker
def get_url(url, connection):
    return connection.get(url).text

inputs = (
    'http://www.google.com/',
    'http://www.yahoo.com/',
)
with vimap.fork(
    get_url(
        connection=vimap.post_fork_closable_arg(requests.Session)
    ),
    range(2)
) as pool:
    outputs = tuple(pool.imap(inputs))

print outputs  # ok, you probably don't really want this ...
```

Behind the scenes, what this will do is,

-   Fork two worker processes

-   On each worker process, create a `requests.Session()` object

-   The main process puts “http://www.google.com/” and “http://www.yahoo.com/”
    on an input queue.

-   The worker processes, when they’re free, will pull these inputs off the
    queue.

-   The worker processes will make the requests, and put the responses on an
    output queue

-   Once the worker processes are done, our `post_fork_closable_arg` decorator
    will close the `requests.Session()` objects.

API overview
------------

### Worker declarations

Workers are instances of `vimap.api.v1.Worker`. This class primarily defines
methods `post_fork_context(worker_index)`, which is a `contextmanager` for
setting up resources and the like on worker startup/shutdown, and
`process(item)`, which maps each input item to an output.

### Worker resources

On top of this base worker interface, we build abstractions for making common
cases convenient. The idea is that you will define a primary work method, which
takes an input and some resources,

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_url(url, connection):
    return connection.get(url).text
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

and then declare the connections. To be a little more explicit than the example,
you could write,

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def make_connection(worker_index):
    connection = requests.Session()
    try:
        yield connection
    finally:
        connection.close()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you decorate `get_url` with `@vimap.fcn_worker`, it creates a function that
will take keyword arguments for instantiating resources, and returns a
`Worker()` instance. Any kwargs that are “tagged” as post-fork context managers
will be used as such on the worker processes, otherwise they’ll be passed
unchanged. For example,

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
@vimap.fcn_worker
def get_url(url, connection, timeout_s):
    return connection.get(url, timeout=timeout_s).text

worker = get_url(
    connection=vimap.PostForkContextManager(make_connection)
    timeout_s=options.timeout_s
)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This instance (`worker`) can then be passed to `fork()`.

### The fork() method

Vimap starts its worker processes when you call `fork()`. Any data afterwards
must be transmitted through FIFO pipes (i.e. data is serialized and
deserialized). The `fork()` method takes two primary arguments,

-   A Worker instance. You can think of this as a tuple of methods:

    -   Initialization — anything that happens in `post_fork_context()` before
        it yields

    -   Mapping — A main method that maps input items to output items

    -   Shutdown / finalization — anything that happens in `post_fork_context()`
        after it yields

-   Iterable of worker indices, `range(multiprocessing.cpu_count())` by default.

As discussed in the second example, after forking,
`worker.post_fork_context(worker_index)` will be called, then
`worker.process(item)` for each input item to process.

The `fork()` method returns a context manager; this is to help clean up the pool
/ ensure that workers get shut down. Other settings (kwargs) to `fork()`
include,

-   `chunk_size` — automatically splits input into chunks of `N` items. If you
    want to process lots of small items, this is a good idea, since it’ll reduce
    communication overhead.

-   `debug` — True/False value to turn on debug output. `vimap` is pretty well
    tested at this point, but if there are internal errors this might help.

-   `max_real_in_flight_factor` / `max_total_in_flight_factor` — both are
    integers that limit the amount of data that can be in vimap’s buffers. The
    defaults are 10 and 100; turn them down if you think queued data is taking
    up too much memory, or you want less input spooling from your lazy input
    iterator. (It could otherwise cost performance, though.)

### Methods on pools

The `fork()` method yields a pool. The basic things to do on a pool are,

-   `pool.imap(input_iter)` — set a lazy iterator as the workers’ inputs.

-   `pool.zip_in_out()` — yields `(input, output)` tuples. The order of the
    input-output pairs will be arbitrary, but each input will be matched to its
    corresponding output.

-   `iter(pool)` — if you just iterate over the pool, iterate over it in a `for`
    loop, call `tuple(pool)`, etc. it will yield the output values in arbitrary
    order from `pool.zip_in_out()`.

Why vimap?
----------

Vimap aspires to be more flexible than `multiprocessing`, and over the course of
its development has ended up being more robust as well. Vimap aspires to support
HTTP-like clients processing data, though contains nothing client-specific.

What in particular makes it more flexible?

-   It facilitates custom initialization of processes (one could connect to a
    HTTP client, etc.), and cleanup.

-   It facilitates configuration passing to processes -- often, with
    multiprocessing, one ends up [wastefully] tupling configuration values with
    every input.

-   It facilitates passing non-serializable objects (before processes are
    forked)

-   Allows timeouts and retrying of failed tasks or processes

What do we aspire to do better than the regular `multiprocessing` library?

-   You don't have to [hack to achieve custom process initialization][1]

    [1]: <http://stackoverflow.com/a/10118250/81636>

-   The API helps prevent dumb mistakes, like [initializing a pool before you've
    defined relevant worker functions][2].

    [2]: <http://stackoverflow.com/q/2782961/81636>

-   Aims to have better worker exception handling --\*\* multiprocessing will
    leave around dead worker processes and FIFOs; we aim not to.\*\*

-   Collection of common use cases (reading from files, etc.)

Other features / design decisions:

-   Attempts to keep all workers busy by pre-emptively enqueuing tasks

-   Also, it *probably* doesn't affect you, but this library can also [work
    around a bug in Python 2.6.7].

### Handling exceptions

If you want to gracefully handle exceptions that bubble up to the main function
of your worker processes, you can request that vimap yield back to you any
exceptions it receives from the workers.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
for input, output, typ in pool.imap(iterable).zip_in_out_typ():
    if typ == 'exception':
        print('Worker had an exception:')
        print(output.formatted_traceback)
    elif typ == 'output':
        print('I got some actual output from a worker!')
        print(output)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`output` will be an `ExceptionContext` namedtuple if the return type is
`exception`; those contain a `value` of the exception raised and
`formatted_traceback` string of the traceback that would have been printed to
stderr.
