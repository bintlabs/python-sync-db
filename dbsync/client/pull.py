"""
Pull, merge and related operations.
"""

from dbsync.lang import *
from dbsync import core
from dbsync.models import ContentType, Operation
from dbsync.messages.pull import PullMessage
from dbsync.client.compression import compress, compressed_operations
from dbsync.client.conflicts import (
    find_direct_conflicts,
    find_dependency_conflicts,
    find_reversed_dependency_conflicts,
    find_insert_conflicts)
from dbsync.client.net import get_request


@core.with_listening(False)
@core.with_transaction
def merge(pull_message, session=None):
    """Merges a message from the server with the local database.

    *pull_message* is an instance of dbsync.messages.pull.PullMessage."""
    if not isinstance(pull_message, PullMessage):
        raise TypeError("need an instance of dbsync.messages.pull.PullMessage "\
                            "to perform the local merge operation")
    content_types = session.query(ContentType).all()
    # preamble: detect conflicts between pulled operations and unversioned ones
    compress()
    unversioned_ops = session.query(Operation).\
        filter(Operation.version_id == None).order_by(Operation.order.asc())
    pull_ops = compressed_operations(pull_message.operations)

    direct_conflicts = find_direct_conflicts(pull_ops, unversioned_ops)

    # in which the delete operation is registered on the pull message
    dependency_conflicts = find_dependency_conflicts(
        pull_ops, unversioned_ops, content_types, session)

    # in which the delete operation was performed locally
    reversed_dependency_conflicts = find_reversed_dependency_conflicts(
        pull_ops, unversioned_ops, content_types, pull_message)

    insert_conflicts = find_insert_conflicts(pull_ops, unversioned_ops)

    # merge transaction
    # first phase: perform pull operations, when allowed and while
    # resolving conflicts
    for pull_op in pull_ops:
        can_perform = True
        if pull_op in imap(fst, direct_conflicts):
            # TODO handle it
            pass
        if pull_op in imap(fst, dependency_conflicts):
            # TODO handle it
            pass
        if pull_op in imap(fst, reversed_dependency_conflicts):
            # TODO handle it
            pass
        if pull_op in imap(fst, insert_conflicts):
            # TODO handle it
            pass

        if can_perform:
            pull_op.perform(content_types,
                            core.synched_models,
                            pull_message,
                            session)

    # second phase: insert versions from the pull_message
    for pull_version in pull_message.versions:
        session.add(pull_version)


class BadResponseError(Exception): pass


def pull(pull_url, extra_data=None):
    """Attempts a pull from the server. Returns the response body.

    Additional data can be passed to the request by giving
    *extra_data*, a dictionary of values.

    The pull operation handling should be configured with specialized
    listeners given by the programmer.

    If not interrupted, the pull will perform a local merge. If the
    response from the server isn't appropriate, it will raise a
    dbysnc.client.pull.BadResponseError."""
    assert isinstance(pull_url, basestring), "pull url must be a string"
    assert bool(pull_url), "pull url can't be empty"
    if extra_data is not None:
        assert isinstance(extra_data, dict), "extra data must be a dictionary"
    extra = dict((k, v) for k, v in extra_data.iteritems()
                 if k != 'latest_version_id') \
                 if extra_data is not None else {}
    data = {'latest_version_id': core.get_latest_version_id()}
    data.update(extra)

    code, reason, response = get_request(pull_url, data)

    if (code // 100 != 2) or response is None:
        raise BadResponseError(code, reason, response)
    message = None
    try:
        message = PullMessage(response)
    except KeyError:
        raise BadResponseError(
            "response object isn't a valid PullMessage", response)
    merge(message)
    # return the response for the programmer to do what she wants
    # afterwards
    return response
