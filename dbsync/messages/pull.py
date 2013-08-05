"""
Pull message and related.
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
from dbsync.models import Operation, Version
from dbsync.messages.base import MessageQuery, BaseMessage
from dbsync.messages.codecs import encode, encode_dict, decode, decode_dict


class PullMessage(BaseMessage):
    """A pull message.

    A pull message can be queried over by version, operation or model,
    and can be filtered multiple times.

    It can be instantiated from a raw data dictionary, or can be made
    empty and filled later with specific methods (``add_version``,
    ``add_operation``, ``add_object``)."""

    #: Datetime of creation.
    created = None

    #: List of operations to perform in the node.
    operations = None

    #: List of versions being pulled.
    versions = None

    def __init__(self, raw_data=None):
        """*raw_data* must be a python dictionary, normally the
        product of JSON decoding. If not given, the message will be
        empty and should be filled with the appropriate methods
        (add_*)."""
        super(PullMessage, self).__init__(raw_data)
        if raw_data is not None:
            self._build_from_raw(raw_data)
        else:
            self.created = datetime.datetime.now()
            self.operations = []
            self.versions = []

    def _build_from_raw(self, data):
        self.created = decode(types.DateTime())(data['created'])
        self.operations = map(partial(object_from_dict, Operation),
                              imap(decode_dict(Operation), data['operations']))
        self.versions = map(partial(object_from_dict, Version),
                            imap(decode_dict(Version), data['versions']))

    def query(self, model):
        """Returns a query object for this message."""
        return MessageQuery(
            model,
            dict(self.payload, **{
                    'models.Operation': self.operations,
                    'models.Version': self.versions}))

    def to_json(self):
        """Returns a JSON-friendly python dictionary. Structure::

            created: datetime,
            operations: list of operations,
            versions: list of versions,
            payload: dictionary with lists of objects mapped to model names
        """
        encoded = super(PullMessage, self).to_json()
        encoded['created'] = encode(types.DateTime())(self.created)
        encoded['operations'] = map(encode_dict(Operation),
                                    imap(properties_dict, self.operations))
        encoded['versions'] = map(encode_dict(Version),
                                  imap(properties_dict, self.versions))
        return encoded

    def add_operation(self, op, session=None):
        """Adds an operation to the message, including the required
        object if it's possible to include it.

        A delete operation doesn't include the associated object. If
        *session* is given, the procedure won't instantiate a new
        session.

        This operation might fail, (due to database inconsitency) in
        which case the internal state of the message won't be affected
        (i.e. it won't end in an inconsistent state)."""
        mname = op.content_type.model_name
        model = synched_models.get(mname, None)
        if model is None:
            raise ValueError("operation linked to model %s "\
                                 "which isn't being tracked" % mname)
        closeit = session is None
        session = session if not closeit else Session()
        obj = query_model(session, model).\
            filter_by(**{get_pk(model): op.row_id}).first() \
            if op.command != 'd' else None
        self.operations.append(op)
        # if the object isn't there it's because the operation is old,
        # and should be able to be compressed out when performing the
        # conflict resolution phase
        if obj is not None:
            self.add_object(obj)
            # add parent objects to resolve possible conflicts in merge
            for parent in parent_objects(obj, synched_models.values(), session):
                self.add_object(parent)
        if closeit:
            session.close()
        return self

    def add_version(self, v, session=None):
        """Adds a version to the message, and all associated
        operations and objects.

        This method will either fail and leave the message instance as
        if nothing had happened, or it will succeed and return the
        modified message."""
        if any(op.content_type.model_name not in synched_models
               for op in v.operations):
            raise ValueError("version includes operation linked "\
                                 "to model not currently being tracked")
        closeit = session is None
        session = Session() if closeit else session
        self.versions.append(v)
        for op in v.operations:
            self.add_operation(op, session=session)
        if closeit:
            session.close()
        return self
