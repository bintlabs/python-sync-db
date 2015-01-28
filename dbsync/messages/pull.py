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

from dbsync.core import (
    Session,
    synched_models,
    pulled_models,
    get_latest_version_id)
from dbsync.models import Operation, Version, ContentType
from dbsync.messages.base import MessageQuery, BaseMessage
from dbsync.messages.codecs import encode, encode_dict, decode, decode_dict


class PullMessage(BaseMessage):
    """
    A pull message.

    A pull message can be queried over by version, operation or model,
    and can be filtered multiple times.

    It can be instantiated from a raw data dictionary, or can be made
    empty and filled later with specific methods (``add_version``,
    ``add_operation``, ``add_object``).
    """

    #: Datetime of creation.
    created = None

    #: List of operations to perform in the node.
    operations = None

    #: List of versions being pulled.
    versions = None

    def __init__(self, raw_data=None):
        """
        *raw_data* must be a python dictionary, normally the
        product of JSON decoding. If not given, the message will be
        empty and should be filled with the appropriate methods
        (add_*).
        """
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
        "Returns a query object for this message."
        return MessageQuery(
            model,
            dict(self.payload, **{
                    'models.Operation': self.operations,
                    'models.Version': self.versions}))

    def to_json(self):
        """
        Returns a JSON-friendly python dictionary. Structure::

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

    def add_operation(self, op, swell=True, session=None):
        """
        Adds an operation to the message, including the required
        object if it's possible to include it.

        If *swell* is given and set to ``False``, the operation and
        object will be added bare, without parent objects. Otherwise,
        the parent objects will be added to aid in conflict
        resolution.

        A delete operation doesn't include the associated object. If
        *session* is given, the procedure won't instantiate a new
        session.

        This operation might fail, (due to database inconsitency) in
        which case the internal state of the message won't be affected
        (i.e. it won't end in an inconsistent state).

        DEPRECATED in favor of `fill_for`
        """
        mname = op.content_type.model_name
        model = synched_models.get(mname, None)
        if model is None:
            raise ValueError("operation linked to model %s "\
                                 "which isn't being tracked" % mname)
        if model not in pulled_models:
            return self
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
            if swell:
                # add parent objects to resolve possible conflicts in merge
                for parent in parent_objects(obj, synched_models.values(),
                                             session):
                    self.add_object(parent)
        if closeit:
            session.close()
        return self

    def add_version(self, v, swell=True, session=None):
        """
        Adds a version to the message, and all associated
        operations and objects.

        This method will either fail and leave the message instance as
        if nothing had happened, or it will succeed and return the
        modified message.

        DEPRECATED in favor of `fill_for`
        """
        for op in v.operations:
            if op.content_type.model_name not in synched_models:
                raise ValueError("version includes operation linked "\
                                 "to model not currently being tracked", op)
        # if any(op.content_type.model_name not in synched_models
        #        for op in v.operations):
        #     raise ValueError("version includes operation linked "\
        #                          "to model not currently being tracked", bad_op)
        closeit = session is None
        session = Session() if closeit else session
        self.versions.append(v)
        for op in v.operations:
            self.add_operation(op, swell=swell, session=session)
        if closeit:
            session.close()
        return self

    def fill_for(self, request, swell=False, include_extensions=True):
        """
        Fills this pull message (response) with versions, operations
        and objects, for the given request (PullRequestMessage).

        Ideally, this method of filling the PullMessage will avoid
        bloating it with parent objects when these aren't required by
        the client node. This effectively means detecting the
        'reversed_dependency_conflicts'
        (:func:`client.conflicts.find_reversed_dependency_conflicts`)
        on the server. This 'smart filling' is disabled if *swell* is
        ``True``.

        *include_extensions* dictates whether the pull message will
        include model extensions or not.
        """
        assert isinstance(request, PullRequestMessage), "invalid request"
        session = Session()
        cts = session.query(ContentType).all()
        versions = session.query(Version)
        if request.latest_version_id is not None:
            versions = versions.\
                filter(Version.version_id > request.latest_version_id)
        for v in versions:
            self.versions.append(v)
            for op in v.operations:
                mname = op.content_type.model_name
                model = synched_models.get(mname, None)
                if model is None:
                    session.close()
                    raise ValueError("operation linked to model %s "\
                                         "which isn't being tracked" % mname)
                if model not in pulled_models: continue
                obj = query_model(session, model).\
                    filter_by(**{get_pk(model): op.row_id}).first() \
                    if op.command != 'd' else None
                self.operations.append(op)
                if obj is None: continue
                self.add_object(obj, include_extensions=include_extensions)
                # add parent objects to resolve conflicts in merge
                for parent in parent_objects(obj, synched_models.values(),
                                             session, only_pk=True):
                    if swell or \
                            any(local_op.references(parent, cts, synched_models)
                                for local_op in request.operations
                                if local_op.command == 'd'):
                        session.expire(parent) # load all attributes at once
                        self.add_object(
                            parent, include_extensions=include_extensions)
        session.close()
        return self


class PullRequestMessage(BaseMessage):
    """
    A pull request message.

    The message includes information for the server to decide whether
    it should send back related (parent) objects to those directly
    involved, for use in conflict resolution, or not. This is used to
    allow for thinner PullMessage(s) to be built, through the
    add_version and add_operation methods.
    """

    #: List of operation the node has performed since the last
    #  synchronization. If empty, the pull is a full 'fast-forward'
    #  thin procedure.
    operations = None

    #: The identifier used to select the operations to be included in
    #  the pull response.
    latest_version_id = None

    def __init__(self, raw_data=None):
        """
        *raw_data* must be a python dictionary. If not given, the
        message should be filled with the or
        add_unversioned_operations method.
        """
        super(PullRequestMessage, self).__init__(raw_data)
        if raw_data is not None:
            self._build_from_raw(raw_data)
        else:
            self.latest_version_id = get_latest_version_id()
            self.operations = []

    def _build_from_raw(self, data):
        self.operations = map(partial(object_from_dict, Operation),
                              imap(decode_dict(Operation), data['operations']))
        self.latest_version_id = decode(types.Integer())(
            data['latest_version_id'])

    def query(self, model):
        "Returns a query object for this message."
        return MessageQuery(
            model,
            dict(self.payload, **{'models.Operation': self.operations}))

    def to_json(self):
        "Returns a JSON-friendly python dictionary."
        encoded = super(PullRequestMessage, self).to_json()
        encoded['operations'] = map(encode_dict(Operation),
                                    imap(properties_dict, self.operations))
        encoded['latest_version_id'] = encode(types.Integer())(
            self.latest_version_id)
        return encoded

    def add_operation(self, op):
        """
        Adds an operation to the message, including the required
        object if possible.
        """
        assert op.version_id is None, "the operation {0} is already versioned".\
            format(op)
        mname = op.content_type.model_name
        model = synched_models.get(mname, None)
        if model is None:
            raise ValueError("operation linked to model %s "\
                                 "which isn't being tracked" % mname)
        if model not in pulled_models: return self
        self.operations.append(op)
        return self

    def add_unversioned_operations(self, session=None):
        """
        Adds all unversioned operations to this message and
        required objects.
        """
        closeit = session is None
        session = Session() if closeit else session
        operations = session.query(Operation).\
            filter(Operation.version_id == None).all()
        if any(op.content_type.model_name not in synched_models
               for op in operations):
            if closeit: session.close()
            raise ValueError("version includes operation linked "\
                                 "to model not currently being tracked")
        for op in operations:
            self.add_operation(op)
        if closeit:
            session.close()
        return self
