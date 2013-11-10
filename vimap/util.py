import functools


def chunk(iterator, chunk_size=100):
    buf = []
    for element in iterator:
        buf.append(element)
        if len(buf) >= chunk_size:
            yield tuple(buf)
            buf = []
    if buf:
        yield tuple(buf)


def instancemethod_runonce(depends=()):
    '''
    Decorator for instance methods that should only run once. This
    should help getting tricky shutdown conditions right.

    Stores its state on an instance's __runonce__ field.
    '''
    assert isinstance(depends, (list, tuple))

    def inner(fcn):
        @functools.wraps(fcn)
        def fcn_helper(self, *args, **kwargs):
            if not hasattr(self, '__runonce__'):
                setattr(self, '__runonce__', {})
            for fcn_name in depends:
                assert self.__runonce__.get(fcn_name), (
                    "Function {0} depends on {1}".format(fcn.__name__, fcn_name))
            if not self.__runonce__.get(fcn.__name__):
                fcn(self, *args, **kwargs)
                self.__runonce__[fcn.__name__] = True
        return fcn_helper
    return inner
