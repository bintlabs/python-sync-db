"""
Push message and related operations.
"""

import datetime

from dbsync.lang import *
from dbsync import core
from dbsync.models import Node, Version
from dbsync.messages.push import PushMessage
from dbsync.client.compression import compress
from dbsync.client.net import post_request


class PushRejected(Exception): pass


@core.with_transaction()
def push(push_url, extra_data=None,
         encode=None, decode=None, headers=None, session=None):
    """Attempts a push to the server. Returns the response body.

    Additional data can be passed to the request by giving
    *extra_data*, a dictionary of values.

    If not interrupted, the push will add a new version to the
    database, and will link all unversioned operations to that newly
    added version.

    If rejected, the push operation will raise a
    dbsync.client.push.PushRejected exception.

    By default, the *encode* function is ``json.dumps``, the *decode*
    function is ``json.loads``, and the *headers* are appropriate HTTP
    headers for JSON."""
    assert isinstance(push_url, basestring), "push url must be a string"
    assert bool(push_url), "push url can't be empty"
    if extra_data is not None:
        assert isinstance(extra_data, dict), "extra data must be a dictionary"
    message = PushMessage()
    message.latest_version_id = core.get_latest_version_id(session)
    compress()
    message.add_unversioned_operations(session)
    message.set_node(session.query(Node).order_by(Node.node_id.desc()).first())
    data = message.to_json()
    data.update({'extra_data': extra_data or {}})

    code, reason, response = post_request(
        push_url, data, encode, decode, headers)

    if (code // 100 != 2) or response is None:
        raise PushRejected(code, reason, response)
    new_version_id = response.get('new_version_id')
    if new_version_id is None:
        raise PushRejected("server didn't respond with new version id", response)
    # Who should set the dates? Maybe send a complete Version from the
    # server. For now the field is ignored, so it doesn't matter.
    session.add(
        Version(version_id=new_version_id, created=datetime.datetime.now()))
    for op in message.operations:
        op.version_id = new_version_id
    # return the response for the programmer to do what she wants
    # afterwards
    return response
