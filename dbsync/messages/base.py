"""
Base functionality for synchronization messages.
"""

import inspect

from dbsync.lang import *
from dbsync.utils import get_pk, properties_dict, construct_bare
from dbsync.core import synched_models, model_extensions
from dbsync import models
from dbsync.messages.codecs import decode_dict, encode_dict


class ObjectType(object):
    """Wrapper for tracked objects."""

    def __init__(self, mname, pk, **kwargs):
        self.__model_name__ = mname
        self.__pk__ = pk
        self.__keys__ = []
        for k, v in kwargs.iteritems():
            if k != '__model_name__' and k != '__pk__' and k != '__keys__':
                setattr(self, k, v)
                self.__keys__.append(k)

    def __repr__(self):
        return u"<ObjectType {0} pk: {1}>".format(
            self.__model_name__, self.__pk__)

    def __eq__(self, other):
        if not isinstance(other, ObjectType):
            raise TypeError("not an instance of ObjectType")
        return self.__model_name__ == other.__model_name__ and \
            self.__pk__ == other.__pk__

    def __hash__(self):
        return self.__pk__

    def to_dict(self):
        return dict((k, getattr(self, k)) for k in self.__keys__)

    def to_mapped_object(self):
        model = synched_models.get(self.__model_name__, None)
        if model is None:
            raise TypeError(
                "model {0} isn't being tracked".format(self.__model_name__))
        obj = construct_bare(model)
        for k in self.__keys__:
            setattr(obj, k, getattr(self, k))
        return obj


class MessageQuery(object):
    """Query over internal structure of a message."""

    def __init__(self, target, payload):
        if target == models.Operation or \
                target == models.Version or \
                target == models.Node:
            self.target = 'models.' + target.__name__
        elif inspect.isclass(target):
            self.target = target.__name__
        elif isinstance(target, basestring):
            self.target = target
        else:
            raise TypeError(
                "query expected a class or string, got %s" % type(target))
        self.payload = payload

    def query(self, model):
        """Returns a new query with a different target, without
        filtering."""
        return MessageQuery(model, self.payload)

    def filter(self, predicate):
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
            else method('to_mapped_object')
        lst = self.payload.get(self.target, None)
        if lst is not None:
            for e in imap(m, lst):
                yield e

    def all(self):
        """Returns a list of all queried objects."""
        return list(self)

    def first(self):
        """Returns the first of the queried objects, or ``None`` if no
        objects matched."""
        try: return next(iter(self))
        except StopIteration: return None


class BaseMessage(object):
    """The base type for messages with a payload."""

    #: dictionary of (model name, set of wrapped objects)
    payload = None

    def __init__(self, raw_data=None):
        self.payload = {}
        if raw_data is not None:
            self._from_raw(raw_data)

    def _from_raw(self, data):
        getm = synched_models.get
        for k, v, m in ifilter(lambda (k, v, m): m is not None,
                               imap(lambda (k, v): (k, v, getm(k, None)),
                                    data['payload'].iteritems())):
            self.payload[k] = set(
                map(lambda dict_: ObjectType(k, dict_[get_pk(m)], **dict_),
                    imap(decode_dict(m), v)))

    def query(self, model):
        """Returns a query object for this message."""
        return MessageQuery(model, self.payload)

    def to_json(self):
        """Returns a JSON-friendly python dictionary."""
        encoded = {}
        encoded['payload'] = {}
        for k, objects in self.payload.iteritems():
            model = synched_models.get(k, None)
            if model is not None:
                encoded['payload'][k] = map(encode_dict(model),
                                            imap(method('to_dict'), objects))
        return encoded

    def add_object(self, obj, include_extensions=True):
        """Adds an object to the message, if it's not already in."""
        class_ = type(obj)
        classname = class_.__name__
        obj_set = self.payload.get(classname, set())
        if ObjectType(classname, getattr(obj, get_pk(class_))) in obj_set:
            return self
        properties = properties_dict(obj)
        if include_extensions:
            for field, ext in model_extensions.get(classname, {}).iteritems():
                _, loadfn, _ = ext
                properties[field] = loadfn(obj)
        obj_set.add(ObjectType(
                classname, getattr(obj, get_pk(class_)), **properties))
        self.payload[classname] = obj_set
        return self
