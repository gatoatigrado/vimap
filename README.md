= Variations on imap, not in C

The `vimap` package is designed to provide a more flexible alternative for `multiprocessing.imap_unordered`. (You should read `multiprocessing` documentation if you haven't already, or else this README won't make sense!) It aspires to support HTTP-like clients processing data, though contains nothing client-specific.

What in particular makes it more flexible?

 * It facilitates custom initialization of processes (one could connect to a HTTP client, etc.), and cleanup.
 * It facilitates configuration passing to processes -- often, with multiprocessing, one ends up [wastefully] tupling configuration values with every input.
 * It facilitates passing non-serializable objects (before processes are forked)
 * Allows timeouts and retrying of failed tasks or processes

What do we aspire to do better than the regular `multiprocessing` library?

 * You don't have to hack to achieve custom process initialization [ c.f. http://stackoverflow.com/q/2080660/81636 ]
 * The API helps prevent dumb mistakes, like initializing a pool before you've defined relevant functions. [ http://stackoverflow.com/q/2782961/81636 ]
 * Aims to have better worker exception handling -- multiprocessing will leave around dead worker processes; we aim not to.
 * Collection of common use cases (reading from files, etc.)

Other features / design decisions:

 * Attempts to keep all workers busy by pre-emptively enqueuing tasks
 * Also, it _probably_ doesn't affect you, but this library can also work around a bug in Python 2.6.7 [ http://stackoverflow.com/q/16684900/81636 ].

== Defining worker functions

`vimap` provides its custom initialization and such via decorated functions. If your inputs are HTTP requests, and you want to get responses from any of a set of servers, you could express your program as such (using the `requests` HTTP library -- it's intuitive so you probably don't need to read its documentation),

    @vimap.worker
    def send_reqests_worker(requests, server):
        s = requests.Session()
        for request in requests:
            yield s.post('http://{0}{1}'.format(server, request.uri), data=request.data)
        s.close()

What is happening? When the worker processes start up, a new session is opened. Each request (some pickleable object containing a .uri and a .data), sent by the parent process, is posted to the server. Then, the worker yields a *single* response, and this response is sent back to the parent process.

== imapping data from the parent process

Let's continue the example,

    pool = vimap.pool(send_requests_worker.init_args(server=server) for server in my_servers)

This initializes a pool of workers. Each one gets a bound argument `server`. When the worker processes start up, they start running until they try to pull an element off of the `requests` iterator; then they must pause for the parent process to send data. The parent process can send data like so,

    Request = namedtuple("Request", ["uri", "data"])
    pool.imap(Request(**ujson.loads(line)) for line in fileinput.input()).ignore_output()

This reads lines from a file containing JSON input, and sends the loaded entries to the workers. In the real world, you'd probably want to make the workers do the JSON loading. The `.ignore_output()` will cause the entire iterable (input file) to be read, and [by default] close the pool after it's done.

=== The input binder

The first Variation on Imap tuples inputs with outputs. So, you have some [lazy] iterable of inputs,

    x1, x2, x3, x4, x5, ...

and when you `vimap` this with some function `f`, you get back tuples,

    (x1, f(x1)), (x2, f(x2)), ...

possibly not in order. Since it's streaming, one shouldn't need to keep around inputs for long -- the input binder will keep around O(# processes) inputs, hence it's safe to iterate through large inputs. In code, this could look like,

    for input, output in pool.imap(iterable).zip_in_out():
        results[input] = output
    # do some more processing
