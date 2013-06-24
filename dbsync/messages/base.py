"""
Base functionality for synchronization messages.
"""    

from dbsync.lang import *
from dbsync.core import synched_models


class ObjectType(object):
    """Represents a tracked object."""

    def __init__(self, mname, **kwargs):
        self.__model_name__ = mname
        self.__keys__ = []
        for k, v in kwargs.iteritems():
            if k != "__model_name__" and k != "__keys__":
                setattr(self, k, v)
                self.__keys__.append(k)

    def __repr__(self):
        return u"<ObjectType {0}>".format(self.__model_name__)

    def to_mapped_object(self):
        model = synched_models.get(self.__model_name__, None)
        if model is None:
            raise TypeError(
                "model {0} isn't being tracked".format(self.__model_name__))
        obj = model()
        for k in self.__keys__:
            setattr(obj, k, getattr(self, k))
        return obj


class MessageQuery(object):
    """Query over internal structure of a message."""

    def __init__(self, target, payload):
        if isinstance(target, models.Operation) or \
                isinstance(target, models.Version) or \
                isinstance(target, models.Node):
            self.target = 'models.' + target.__class__.__name__
        elif not isinstance(target, basestring):
            self.target = target.__class__.__name__
        else:
            self.target = target
        self.payload = payload

    def query(self, model):
        """Returns a new query with a different target, without
        filtering."""
        return MessageQuery(model, self.payload)

    def filter(predicate):
        """Returns a new query with the collection filtered according
        to the predicate applied to the target objects."""
        to_filter = self.payload.get(self.target, None)
        if to_filter is None:
            return self
        return MessageQuery(
            self.target,
            dict(self.payload, **{self.target: filter(predicate, to_filter)}))

    def __iter__(self):
        """Yields objects mapped to their original type (*target*)."""
        m = identity if self.target.startswith('models.') \
            else method("to_mapped_object")
        lst = self.payload.get(self.target, None)
        if lst is not None:
            for e in imap(m, lst):
                yield e
