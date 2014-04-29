"""
Push message and related.
"""

import datetime
import hashlib

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
from dbsync.messages.base import MessageQuery, BaseMessage
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
    explicitly as well. The method ``set_node`` is required to be used
    for proper key generation.

    If the node is not assigned the message will still behave
    normally, since verification of its presence is not enforced on
    the client, and might not be enforced on the server. Likewise, if
    the latest version isn't assigned, it'll be just interpreted on
    the server to be the initial data load.

    To verify correctness, use ``islegit`` giving a session with
    access to the synch database."""

    #: Datetime of creation
    created = None

    #: Node primary key
    node_id = None

    #: Secret used internally to mitigate obnoxiousness.
    _secret = None

    #: Key to this message
    key = None

    #: The latest version
    latest_version_id = None

    #: List of unversioned operations
    operations = None

    def __init__(self, raw_data=None):
        """*raw_data* must be a python dictionary. If not given, the
        message will be empty and should be filled after
        instantiation."""
        super(PushMessage, self).__init__(raw_data)
        if raw_data is not None:
            self._build_from_raw(raw_data)
        else:
            self.created = datetime.datetime.now()
            self.operations = []

    def _build_from_raw(self, data):
        self.created = decode(types.DateTime())(data['created'])
        self.node_id = decode(types.Integer())(data['node_id'])
        self.key = decode(types.String())(data['key'])
        self.latest_version_id = decode(types.Integer())(
            data['latest_version_id'])
        self.operations = map(partial(object_from_dict, Operation),
                              imap(decode_dict(Operation), data['operations']))

    def query(self, model):
        """Returns a query object for this message."""
        return MessageQuery(
            model,
            dict(
                self.payload,
                **{'models.Operation': self.operations}))

    def to_json(self):
        """Returns a JSON-friendly python dictionary. Structure::

            created: datetime,
            node_id: node primary key or null,
            key: a string generated from the secret and part of the message,
            latest_version_id: number or null,
            operations: list of operations,
            payload: dictionay with lists of objects mapped to model names
        """
        encoded = super(PushMessage, self).to_json()
        encoded['created'] = encode(types.DateTime())(self.created)
        encoded['node_id'] = encode(types.Integer())(self.node_id)
        encoded['key'] = encode(types.String())(self.key)
        encoded['latest_version_id'] = encode(types.Integer())(
            self.latest_version_id)
        encoded['operations'] = map(encode_dict(Operation),
                                    imap(properties_dict, self.operations))
        return encoded

    def _portion(self):
        """Returns part of this message as a string."""
        portion = "".join("&{0}#{1}#{2}".\
                              format(op.row_id, op.content_type_id, op.command)
                          for op in self.operations)
        return portion

    def _sign(self):
        if self._secret is not None:
            self.key = hashlib.sha512(self._secret + self._portion()).hexdigest()

    def set_node(self, node):
        """Sets the node and key for this message."""
        if node is None: return
        self.node_id = node.node_id
        self._secret = node.secret
        self._sign()

    def islegit(self, session):
        """Checks whether the key for this message is proper."""
        if self.key is None or self.node_id is None: return False
        node = session.query(Node).filter(Node.node_id == self.node_id).one()
        return self.key == hashlib.sha512(node.secret + self._portion()).\
            hexdigest()

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
            # parent objects aren't added, because the merge (and it's
            # conflicts) ocurr solely on the pull operation

            # for parent in parent_objects(obj, synched_models.values(), session):
            #     self.add_object(parent)
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
        if self.key is not None:
            # overwrite since it's probably an incorrect key
            self._sign()
        return self
