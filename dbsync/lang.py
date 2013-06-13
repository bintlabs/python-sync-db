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


class Function(object):
    """Composable function for attr and method usage."""
    def __init__(self, fn):
        self.fn = fn
    def __call__(self, obj):
        return self.fn(obj)
    def __eq__(self, other):
        return Function(lambda obj: self.fn(obj) == other)
    def __lt__(self, other):
        return Function(lambda obj: self.fn(obj) < other)
    def __le__(self, other):
        return Function(lambda obj: self.fn(obj) <= other)
    def __ne__(self, other):
        return Function(lambda obj: self.fn(obj) != other)
    def __gt__(self, other):
        return Function(lambda obj: self.fn(obj) > other)
    def __ge__(self, other):
        return Function(lambda obj: self.fn(obj) >= other)


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
