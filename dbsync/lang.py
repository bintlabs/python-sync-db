"""
Generic functions for repeating patterns.
"""

from itertools import imap, ifilter, starmap
from functools import partial, wraps


def begin(arg, *args):
    """Sequencing, for use in lambdas."""
    if not args:
        return arg
    return args[-1]


def identity(x):
    return x


def const(value):
    return lambda *args, **kwargs: value


def fst(pair):
    return pair[0]


def snd(pair):
    return pair[1]


def swap(pair):
    f, s = pair
    return (s, f)


def partition(predicate, collection):
    """Splits the collection according to the predicate.

    Returns a pair of (true-tested, false-tested). Evaluating it is
    equivalent to evaluating ``(filter(predicate, collection),
    filter(lambda e: not predicate(e), collection))``"""
    positives = []
    negatives = []
    for e in collection:
        if predicate(e):
            positives.append(e)
        else:
            negatives.append(e)
    return (positives, negatives)


def maybe(value, fn=identity, default=""):
    """``if value is None: ...`` more compressed."""
    if value is None:
        return default
    return fn(value)


def guard(f):
    """Propagate nothingness in a function of one argument."""
    @wraps(f)
    def g(x):
        return maybe(x, f, None)
    return g


class Function(object):
    """Composable function for attr and method usage."""
    def __init__(self, fn):
        self.fn = fn
        self.__name__ = fn.__name__ # e.g. for the wraps decorator
    def __call__(self, obj):
        return self.fn(obj)
    def __eq__(self, other):
        if isinstance(other, Function):
            return Function(lambda obj: self.fn(obj) == other(obj))
        else:
            return Function(lambda obj: self.fn(obj) == other)
    def __lt__(self, other):
        if isinstance(other, Function):
            return Function(lambda obj: self.fn(obj) < other(obj))
        else:
            return Function(lambda obj: self.fn(obj) < other)
    def __le__(self, other):
        if isinstance(other, Function):
            return Function(lambda obj: self.fn(obj) <= other(obj))
        else:
            return Function(lambda obj: self.fn(obj) <= other)
    def __ne__(self, other):
        if isinstance(other, Function):
            return Function(lambda obj: self.fn(obj) != other(obj))
        else:
            return Function(lambda obj: self.fn(obj) != other)
    def __gt__(self, other):
        if isinstance(other, Function):
            return Function(lambda obj: self.fn(obj) > other(obj))
        else:
            return Function(lambda obj: self.fn(obj) > other)
    def __ge__(self, other):
        if isinstance(other, Function):
            return Function(lambda obj: self.fn(obj) >= other(obj))
        else:
            return Function(lambda obj: self.fn(obj) >= other)
    def __invert__(self):
        return Function(lambda obj: not self.fn(obj))
    def __and__(self, other):
        if isinstance(other, Function):
            return Function(lambda obj: self.fn(obj) and other(obj))
        else:
            return Function(lambda obj: self.fn(obj) and other)
    def __or__(self, other):
        if isinstance(other, Function):
            return Function(lambda obj: self.fn(obj) or other(obj))
        else:
            return Function(lambda obj: self.fn(obj) or other)


def attr(name):
    """For use in standard higher order functions."""
    return Function(lambda obj: getattr(obj, name))


def method(name, *args, **kwargs):
    """For use in standard higher order functions."""
    return Function(lambda obj: getattr(obj, name)(*args, **kwargs))


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


def group_by(fn, col):
    """Groups a collection according to the given *fn* into a dictionary.

    *fn* should return a hashable. """
    groups = {}
    for e in col:
        key = fn(e)
        subcol = groups.get(key, None)
        if subcol is None:
            groups[key] = [e]
        else:
            subcol.append(e)
    return groups


def lookup(predicate, collection, default=None):
    """Looks up the first value in *collection* that satisfies
    *predicate*."""
    for e in collection:
        if predicate(e):
            return e
    return default
