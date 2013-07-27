"""
Push message and related.
"""

import datetime

from sqlalchemy import types
from dbsync.utils import (
    properties_dict,
    object_from_dict,
    get_pk,
    parent_objects,
    query_model)
from dbsync.lang import *

from dbsync.core import Session, synched_models
from dbsync.models import Node, Operation
from dbsync.messages.base import ObjectType, MessageQuery, BaseMessage
from dbsync.messages.codecs import encode, encode_dict, decode, decode_dict


class PushMessage(BaseMessage):
    """A push message.

    A push message contains the latest version information, the node
    information, and the list of unversioned operations and the
    required objects for those to be performed.

    The message can be instantiated from a raw data dictionary or can
    be made empty and filled later with the
    ``add_unversioned_operations`` method, in which case the node
    attribute and the latest version identifier should be assigned
    explicitly as well.

    If the node is not assigned the message will still behave
    normally, since verification of its presence is not enforced on
    the client, and could not be enforced on the server. Likewise, if
    the latest version isn't assigned, it'll be just interpreted on
    the server to be the initial data load."""

    #: Datetime of creation
    created = None

    #: Node information
    node = None

    #: The latest version
    latest_version_id = None

    #: List of unversioned operations
    operations = None

    def __init__(self, raw_data=None):
        """*raw_data* must be a python dictionary. If not given, the
        message will be empty and should be filled after
        instantiation."""
        super(PushMessage, self).__init__()
        if raw_data is not None:
            self._build_from_raw(raw_data)
        else:
            self.created = datetime.datetime.now()
            self.operations = []

    def _build_from_raw(self, data):
        self.created = decode(types.DateTime())(data['created'])
        decode_node = lambda dict_: object_from_dict(Node,
                                                     decode_dict(Node)(dict_))
        self.node = guard(decode_node)(data['node'])
        self.latest_version_id = decode(types.Integer())(
            data['latest_version_id'])
        self.operations = map(partial(object_from_dict, Operation),
                              imap(decode_dict(Operation), data['operations']))
        getm = synched_models.get
        for k, v, m in ifilter(lambda (k, v, m): m is not None,
                               imap(lambda (k, v): (k, v, getm(k, None)),
                                    data['payload'].iteritems())):
            self.payload[k] = set(
                map(lambda dict_: ObjectType(k, dict_[get_pk(m)], **dict_),
                    imap(decode_dict(m), v)))

    def query(self, model):
        """Returns a query object for this message."""
        return MessageQuery(
            model,
            dict(
                self.payload,
                **{'models.Operation': self.operations,
                   'models.Node': [self.node] if self.node is not None else []}))

    def to_json(self):
        """Returns a JSON-friendly python dictionary. Structure::

            created: datetime,
            node: node object or null,
            latest_version_id: number or null,
            operations: list of operations,
            payload: dictionay with lists of objects mapped to model names
        """
        encoded = {}
        encoded['created'] = encode(types.DateTime())(self.created)
        encoded['node'] = encode_dict(Node)(properties_dict(self.node)) \
            if self.node is not None else None
        encoded['latest_version_id'] = encode(types.Integer())(
            self.latest_version_id)
        encoded['operations'] = map(encode_dict(Operation),
                                    imap(properties_dict, self.operations))
        encoded['payload'] = {}
        for k, objects in self.payload.iteritems():
            model = synched_models.get(k, None)
            if model is not None:
                encoded['payload'][k] = map(encode_dict(model),
                                            imap(method("to_dict"), objects))
        return encoded

    def _add_operation(self, op, session):
        mname = op.content_type.model_name
        model = synched_models.get(mname, None)
        if model is None:
            raise ValueError("operation linked to model %s "\
                                 "which isn't being tracked" % mname)
        obj = query_model(session, model).\
            filter_by(**{get_pk(model): op.row_id}).first() \
            if op.command != 'd' else None
        self.operations.append(op)
        if obj is not None:
            self.add_object(obj)
            # for possible conflicts in merge
            for parent in parent_objects(obj, synched_models.values(), session):
                self.add_object(parent)
        return self

    def add_unversioned_operations(self, session=None):
        """Adds all unversioned operations to this message, including
        the required objects for them to be performed."""
        closeit = session is None
        session = Session() if closeit else session
        operations = session.query(Operation).\
            filter(Operation.version_id == None).all()
        if any(op.content_type.model_name not in synched_models
               for op in operations):
            session.close()
            raise ValueError("version includes operation linked "\
                                 "to model not currently being tracked")
        for op in operations:
            self._add_operation(op, session)
        if closeit:
            session.close()
        return self
