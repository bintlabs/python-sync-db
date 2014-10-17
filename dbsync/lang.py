"""
Generic functions for repeating patterns.
"""

from itertools import imap, ifilter, starmap, izip, dropwhile, takewhile
from functools import partial as partial_apply, wraps


def begin(arg, *args):
    "Sequencing, for use in lambdas."
    if not args:
        return arg
    return args[-1]


def identity(x):
    return x


def const(value):
    return lambda *args, **kwargs: value


def index(ind):
    return lambda indexable: indexable[ind]

fst = index(0)
snd = index(1)


def swap(pair):
    f, s = pair
    return (s, f)


def partition(predicate, collection):
    """
    Splits the collection according to the predicate.

    Returns a pair of (true-tested, false-tested). Evaluating it is
    equivalent to evaluating ``(filter(predicate, collection),
    filter(lambda e: not predicate(e), collection))``
    """
    positives, negatives = [], []
    for e in collection:
        (positives if predicate(e) else negatives).append(e)
    return (positives, negatives)


def maybe(value, fn=identity, default=""):
    "``if value is None: ...`` more compressed."
    if value is None:
        return default
    return fn(value)


def guard(f):
    "Propagate nothingness in a function of one argument."
    @wraps(f)
    def g(x):
        return maybe(x, f, None)
    return g


def partial(f, *arguments):
    """
    http://bugs.python.org/issue3445
    https://docs.python.org/2/library/functools.html#partial-objects
    """
    p = partial_apply(f, *arguments)
    p.__module__ = f.__module__
    p.__name__ = "partial-{0}".format(f.__name__)
    return p


class Function(object):
    "Composable function for attr and method usage."
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
    def in_(self, collection):
        return Function(lambda obj: self.fn(obj) in collection)


def attr(name):
    "For use in standard higher order functions."
    return Function(lambda obj: getattr(obj, name))


def method(name, *args, **kwargs):
    "For use in standard higher order functions."
    return Function(lambda obj: getattr(obj, name)(*args, **kwargs))


def group_by(fn, col):
    """
    Groups a collection according to the given *fn* into a dictionary.

    *fn* should return a hashable.
    """
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
    """
    Looks up the first value in *collection* that satisfies
    *predicate*.
    """
    for e in collection:
        if predicate(e):
            return e
    return default


def mfilter(predicate, lst):
    """
    Removes the elements in *lst* that don't satisfy *predictate*,
    mutating *lst* (a list or a set).
    """
    matching = filter(lambda e: not predicate(e), lst)
    for e in matching:
        lst.remove(e)
    return lst
