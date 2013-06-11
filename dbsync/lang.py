"""
Generic functions for repeating patterns.
"""


def begin(arg, *args):
    """Sequencing, for use in lambdas."""
    if not args:
        return arg
    return args[-1]


def identity(x):
    return x


def const(value):
    return lambda *args, **kwargs: value


def maybe(value, fn=identity, default=""):
    """``if value is None: ...`` more compressed."""
    if value is None:
        return default
    return fn(value)


def attr(name):
    """For use in standard higher order functions."""
    return lambda obj: getattr(obj, name)


def method(name, *args, **kwargs):
    """For use in standard higher order functions."""
    return lambda obj: getattr(obj, name)(*args, **kwargs)


def andmap(predicate, collection, *collections):
    """Map predicate and reduce with and, short circuiting."""
    iters = map(iter, collections)
    for elem in collection:
        elems = map(next, iters)
        if not predicate(elem, *elems):
            return False
    return True


def ormap(predicate, collection, *collections):
    """Map predicate and reduce with or, short circuiting."""
    iters = map(iter, collections)
    for elem in collection:
        elems = map(next, iters)
        if predicate(elem, *elems):
            return True
    return False
