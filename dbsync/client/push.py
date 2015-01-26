"""
Push message and related operations.
"""

import datetime

from dbsync.lang import *
from dbsync import core
from dbsync.models import Node, Version
from dbsync.messages.push import PushMessage
from dbsync.client.compression import compress
from dbsync.client.register import get_node
from dbsync.client.net import post_request


class PushRejected(Exception): pass

class PullSuggested(PushRejected): pass


# user-defined predicate to decide based on the server's response
suggests_pull = None


@core.with_transaction()
def request_push(push_url, message, extra_data=None,
                 encode=None, decode=None, headers=None, timeout=None,
                 session=None):
    data = message.to_json()
    data.update({'extra_data': extra_data or {}})

    code, reason, response = post_request(
        push_url, data, encode, decode, headers, timeout)

    if (code // 100 != 2) or response is None:
        if suggests_pull is not None and suggests_pull(code, reason, response):
            raise PullSuggested(code, reason, response)
        raise PushRejected(code, reason, response)
    new_version_id = response.get('new_version_id')
    if new_version_id is None:
        raise PushRejected(
            code,
            reason,
            {'error': "server didn't respond with new version id",
             'response': response})
    # Who should set the dates? Maybe send a complete Version from the
    # server. For now the field is ignored, so it doesn't matter.
    session.add(
        Version(version_id=new_version_id, created=datetime.datetime.now()))
    for op in message.operations:
        session.merge(op).version_id = new_version_id
    # return the response for the programmer to do what she wants
    # afterwards
    return response


def push(push_url, extra_data=None,
         encode=None, decode=None, headers=None, timeout=None,
         include_extensions=True):
    """
    Attempts a push to the server. Returns the response body.

    Additional data can be passed to the request by giving
    *extra_data*, a dictionary of values.

    If not interrupted, the push will add a new version to the
    database, and will link all unversioned operations to that newly
    added version.

    If rejected, the push operation will raise a
    dbsync.client.push.PushRejected exception.

    By default, the *encode* function is ``json.dumps``, the *decode*
    function is ``json.loads``, and the *headers* are appropriate HTTP
    headers for JSON.

    *include_extensions* dictates whether the message will include
    model extensions or not.
    """
    assert isinstance(push_url, basestring), "push url must be a string"
    assert bool(push_url), "push url can't be empty"
    if extra_data is not None:
        assert isinstance(extra_data, dict), "extra data must be a dictionary"
    message = PushMessage()
    message.latest_version_id = core.get_latest_version_id()
    compress()
    message.add_unversioned_operations(include_extensions=include_extensions)
    message.set_node(get_node())

    return request_push(
        push_url, message, extra_data=extra_data,
        encode=encode, decode=decode, headers=headers, timeout=timeout,
        include_extensions=include_extensions)
