"""
Registry, pull, push and other request handlers.

The pull cycle consists in receiving a version identifier and sending
back a PullMessage filled with versions above the one received.

The push cycle consists in receiving a complete PushMessage and either
rejecting it based on latest version or signature, or accepting it and
performing the operations indicated in it. The operations should also
be inserted in the operations table, in the correct order but getting
new keys for the 'order' column, and linked with a newly created
version. If it accepts the message, the push handler should also
return the new version identifier to the node (and the programmer is
tasked to send the HTTP response).
"""

import datetime

from sqlalchemy.orm import make_transient

from dbsync.lang import *
from dbsync.utils import (
    generate_secret,
    properties_dict,
    column_properties,
    get_pk,
    query_model,
    EventRegister)
from dbsync import core
from dbsync.models import (
    Version,
    Node,
    OperationError,
    Operation)
from dbsync.messages.base import BaseMessage
from dbsync.messages.register import RegisterMessage
from dbsync.messages.pull import PullMessage, PullRequestMessage
from dbsync.messages.push import PushMessage
from dbsync.server.conflicts import find_unique_conflicts
from dbsync.logs import get_logger


logger = get_logger(__name__)


@core.session_closing
def handle_query(data, session=None):
    "Responds to a query request."
    model = core.synched_models.model_names.\
        get(data.get('model', None), core.null_model).model
    if model is None: return None
    mname = model.__name__
    filters = dict((k, v) for k, v in ((k[len(mname) + 1:], v)
                                       for k, v in data.iteritems()
                                       if k.startswith(mname + '_'))
                   if k and k in column_properties(model))
    message = BaseMessage()
    q = query_model(session, model)
    if filters:
        q = q.filter_by(**filters)
    for obj in q:
        message.add_object(obj)
    return message.to_json()


@core.session_closing
def handle_repair(data=None, session=None):
    "Handle repair request. Return whole server database."
    include_extensions = 'exclude_extensions' not in (data or {})
    latest_version_id = core.get_latest_version_id(session=session)
    message = BaseMessage()
    for model in core.synched_models.models.iterkeys():
        for obj in query_model(session, model):
            message.add_object(obj, include_extensions=include_extensions)
    response = message.to_json()
    response['latest_version_id'] = latest_version_id
    return response


@core.with_transaction()
def handle_register(user_id=None, node_id=None, session=None):
    """
    Handle a registry request, creating a new node, wrapping it in a
    message and returning it to the client node.

    *user_id* can be a numeric key to a user record, which will be set
    in the node record itself.

    If *node_id* is given, it will be used instead of creating a new
    node. This allows for node reuse according to criteria specified
    by the programmer.
    """
    message = RegisterMessage()
    if node_id is not None:
        node = session.query(Node).filter(Node.node_id == node_id).first()
        if node is not None:
            message.node = node
            return message.to_json()
    newnode = Node()
    newnode.registered = datetime.datetime.now()
    newnode.registry_user_id = user_id
    newnode.secret = generate_secret(128)
    session.add(newnode)
    session.flush()
    message.node = newnode
    return message.to_json()


class PullRejected(Exception): pass


def handle_pull(data, swell=False, include_extensions=True):
    """
    Handle the pull request and return a dictionary object to be sent
    back to the node.

    *data* must be a dictionary-like object, usually one obtained from
    decoding a JSON dictionary in the POST body.
    """
    try:
        request_message = PullRequestMessage(data)
    except KeyError:
        raise PullRejected("request object isn't a valid PullRequestMessage", data)

    message = PullMessage()
    message.fill_for(
        request_message,
        swell=swell,
        include_extensions=include_extensions)
    return message.to_json()


class PushRejected(Exception): pass

class PullSuggested(PushRejected): pass


#: Callbacks receive the session and the message.
before_push = EventRegister()
after_push = EventRegister()


@core.with_transaction()
def handle_push(data, session=None):
    """
    Handle the push request and return a dictionary object to be sent
    back to the node.

    If the push is rejected, this procedure will raise a
    dbsync.server.handlers.PushRejected exception.

    *data* must be a dictionary-like object, usually the product of
    parsing a JSON string.
    """
    message = None
    try:
        message = PushMessage(data)
    except KeyError:
        raise PushRejected("request object isn't a valid PushMessage", data)
    latest_version_id = core.get_latest_version_id(session=session)
    if latest_version_id != message.latest_version_id:
        exc = "version identifier isn't the latest one; "\
            "given: %s" % message.latest_version_id
        if latest_version_id is None:
            raise PushRejected(exc)
        if message.latest_version_id is None:
            raise PullSuggested(exc)
        if message.latest_version_id < latest_version_id:
            raise PullSuggested(exc)
        raise PushRejected(exc)
    if not message.operations:
        raise PushRejected("message doesn't contain operations")
    if not message.islegit(session):
        raise PushRejected("message isn't properly signed")

    for listener in before_push:
        listener(session, message)

    # I) detect unique constraint conflicts and resolve them if possible
    unique_conflicts = find_unique_conflicts(message, session)
    conflicting_objects = set()
    for uc in unique_conflicts:
        obj = uc['object']
        conflicting_objects.add(obj)
        for key, value in izip(uc['columns'], uc['new_values']):
            setattr(obj, key, value)
    for obj in conflicting_objects:
        make_transient(obj) # remove from session
    for model in set(type(obj) for obj in conflicting_objects):
        pk_name = get_pk(model)
        pks = [getattr(obj, pk_name)
               for obj in conflicting_objects
               if type(obj) is model]
        session.query(model).filter(getattr(model, pk_name).in_(pks)).\
            delete(synchronize_session=False) # remove from the database
    session.add_all(conflicting_objects) # reinsert
    session.flush()

    # II) perform the operations
    try:
        for op in ifilter(lambda o: o.tracked_model is not None,
                          message.operations):
            op.perform(message, session, message.node_id)
    except OperationError as e:
        logger.exception(u"Couldn't perform operation in push from node %s.",
                         message.node_id)
        raise PushRejected("at least one operation couldn't be performed",
                           *e.args)

    # III) insert a new version
    version = Version(created=datetime.datetime.now(), node_id=message.node_id)
    session.add(version)

    # IV) insert the operations, discarding the 'order' column
    for op in sorted(message.operations, key=attr('order')):
        new_op = Operation()
        for k in ifilter(lambda k: k != 'order', properties_dict(op)):
            setattr(new_op, k, getattr(op, k))
        session.add(new_op)
        new_op.version = version
        session.flush()

    for listener in after_push:
        listener(session, message)

    # return the new version id back to the node
    return {'new_version_id': version.version_id}
