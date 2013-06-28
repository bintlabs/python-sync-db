"""
Pull message and related.
"""

import datetime

from sqlalchemy import types
from dbsync.utils import properties_dict, types_dict, object_from_dict, get_pk
from dbsync.lang import *

from dbsync.core import Session, synched_models
from dbsync.core.models import ContentType, Operation, Version
from dbsync.message.base import ObjectType, MessageQuery
from dbsync.message.codecs import encode, decode


def encode_dict(class_):
    """Returns a function that transforms a dictionary, mapping the
    types to simpler ones, according to the given mapped class."""
    types = types_dict(class_)
    encodings = dict((k, encode(t)) for k, t in types.iteritems())
    return lambda dict_: dict((k, encodings[k](v)) for k, v in dict_.iteritems())


def decode_dict(class_):
    """Returns a function that transforms a dictionary, mapping the
    types to richer ones, according to the given mapped class."""
    types = types_dict(class_)
    decodings = dict((k, decode(t)) for k, t in types.iteritems())
    return lambda dict_: dict((k, decodings[k](v)) for k, v in dict_.iteritems())


class PullMessage(object):
    """A pull message.

    A pull message can be queried over by version, operation or model,
    and can be filtered multiple times.

    It can be instantiated from a raw data dictionary, or can be made
    empty and filled later with specific methods (add_version,
    add_operation, add_object)."""

    #: Datetime of creation.
    created = None

    #: List of operations to perform in the node.
    operations = []

    #: List of versions being pulled.
    versions = []

    #: Extra data required to perform the operations.
    #: dictionary of (model name, set of wrapped objects)
    payload = {}

    def __init__(self, raw_data=None):
        """*raw_data* must be a python dictionary, normally the
        product of JSON decoding. If not given, the message will be
        empty and should be filled with the appropriate methods
        (add_*)."""
        if raw_data is not None:
            self._build_from_raw(raw_data)
        else:
            self.created = datetime.datetime.now()

    def _build_from_raw(self, data):
        self.created = decode(types.DateTime)(data['created'])
        self.operations = map(partial(object_from_dict, Operation),
                              imap(decode_dict(Operation), data['operations']))
        self.versions = map(partial(object_from_dict, Version),
                            imap(decode_dict(Version), data['versions']))
        getm = synched_models.get
        for k, v, m in ifilter(lambda (k, v, m): m is not None,
                               imap(lambda (k, v): (k, v, getm(k, None)),
                                    raw_data['payload'].iteritems())):
            self.payload[k] = set(
                map(lambda dict_: ObjectType(k, dict_[get_pk(m)], **dict_),
                    imap(decode_dict(m), v)))

    def query(self, model):
        """Returns a query object for this message."""
        return MessageQuery(
            model,
            dict(self.payload, **{
                    'models.Operation': self.operations,
                    'models.Version': self.versions}))

    def encode(self):
        """To JSON-friendly python dictionary. Structure::

            created: date,
            operations: list of operations,
            versions: list of versions,
            payload: dictionary with lists of objects mapped to model names
        """
        encoded = {}
        encoded['created'] = encode(types.DateTime)(self.created)
        encoded['operations'] = map(encode(Operation),
                                    imap(properties_dict, self.operations))
        encoded['versions'] = map(encode(Version),
                                  imap(properties_dict, self.versions))
        encoded['payload'] = {}
        for k, objects in self.payload.iteritems():
            model = synched_models.get(k, None)
            if model is not None:
                encoded['payload'][k] = map(encode_dict(model),
                                            imap(method("to_dict"), objects))
        return encoded

    def add_object(self, obj):
        """Adds an object to the message, if it's not already in."""
        class_ = obj.__class__
        classname = class_.__name__
        obj_set = self.payload.get(classname, set())
        obj_set.add(ObjectType(
                classname, getattr(obj, get_pk(class_)), properties_dict(obj)))
        self.payload[classname] = obj_set
        return self

    def add_operation(self, op, include_object=True, session=None):
        """Adds an operation to the message.

        If *include_object* is ``True`` (the default), the required
        object for it to be performed will be added to the message."""
        mname = op.content_type.model_name
        model = synched_models.get(mname, None)
        if model is None:
            raise ValueError("operation linked to a model "\
                                 "that's not being tracked: %s" % mname)
        self.operations.append(op)
        if op.command == 'd' or not include_object:
            return self # can't include the object for delete commands
        closeit = session is None
        session = session if not closeit else Session()
        obj = session.query(model).\
            filter_by(**{get_pk(model): op.row_id}).first()
        if obj is None:
            raise ValueError("operation linked to an object "\
                                 "not present in database: %s" % op)
        self.add_object(obj)
        if closeit:
            session.close()
        return self

    def add_version(self, v, include_operations=True, include_objects=True):
        """Adds a version to the message.

        If *include_operations* is ``True``, it also adds all
        operations matching the given version. If *include_objects* is
        ``True``, it also will add the objects required by those
        operations. Both are ``True`` by default."""
        self.versions.append(v)
        if not include_operations:
            return self
        session = None if include_objects else Session()
        for op in v.operations:
            self.add_operation(
                op, include_object=include_objects, session=session)
        session.close()
        return self
