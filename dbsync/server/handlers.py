"""
Pull and push request handlers.

The pull cycle consists in receiving a version identifier and sending
back a PullMessage filled with versions above the one received.

The push cycle consists in receiving a complete PushMessage and either
rejecting it based on latest version, or accepting it and performing
the operations indicated in it. The operations should also be inserted
in the operations table, in the correct order but getting new keys for
the 'order' column, and linked with a newly created version. If it
accepts the message, the push handler should also return the new
version identifier to the node (and the programmer is tasked to send
the HTTP response).
"""

import datetime

from dbsync.utils import properties_dict
from dbsync import core
from dbsync.models import (
    Version,
    Node,
    ContentType,
    OperationError,
    Operation)
from dbsync.messages.pull import PullMessage
from dbsync.messages.push import PushMessage


def handle_pull(data):
    """Handle the pull request and return a dictionary object to be
    sent back to the node.

    *data* must be a dictionary-like object, usually one containing
    the GET parameters of the request."""
    session = core.Session()
    latest_version_id = data.get('latest_version_id', None)
    versions = session.query(Version)
    if latest_version_id is not None:
        versions = versions.filter(Version.version_id > latest_version_id)
    message = PullMessage()
    for v in versions:
        message.add_version(v, session=session)
    session.close()
    return message.to_json()


class PushRejected(Exception): pass


@core.with_listening(False)
@core.with_transaction
def handle_push(data, session=None):
    """Handle the push request and return a dictionary object to be
    sent back to the node.

    If the push is rejected, this procedure will raise a
    dbsync.server.handlers.PushRejected exception.

    *data* must be a dictionary-like object, usually the product of
    parsing a JSON string."""
    message = None
    try:
        message = PushMessage(data)
    except KeyError:
        raise PushRejected("request object isn't a valid PushMessage", data)
    latest_version_id = core.get_latest_version_id(session)
    if latest_version_id != message.latest_version_id:
        raise PushRejected("version identifier isn't the latest one; "\
                               "given: %d" % message.latest_version_id)
    # ensure the node given exists in database
    if message.node is None:
        raise PushRejected("sender node is not specified")
    node = session.query(Node).\
        filter(Node.node_id == message.node.node_id).first()
    if node is None or properties_dict(node) != properties_dict(message.node):
        raise PushRejected("sender node isn't registered in the server")
    # perform the operations
    try:
        content_types = session.query(ContentType).all()
        for op in message.operations:
            op.perform(content_types, core.synched_models, message, session)
    except OperationError as e:
        raise PushRejected("at least one operation couldn't be performed",
                           *e.args)
    # insert a new version
    version = Version(created=datetime.datetime.now())
    session.add(version)
    # insert the operations, discarding the 'order' column
    for op in sorted(message.operations, key=attr("order")):
        new_op = Operation()
        for k in ifilter(lambda k: k != 'order', properties_dict(op)):
            setattr(new_op, k, getattr(op, k))
        session.add(new_op)
        new_op.version = version
        session.flush()
    # return the new version id back to the node
    return {'new_version_id': version.version_id}
