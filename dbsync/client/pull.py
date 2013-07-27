"""
Pull, merge and related operations.
"""

from dbsync.lang import *
from dbsync.utils import get_pk, query_model
from dbsync import core
from dbsync.messages.pull import PullMessage
from dbsync.client.compression import compress, compressed_operations
from dbsync.client.conflicts import (
    find_direct_conflicts,
    find_dependency_conflicts,
    find_reversed_dependency_conflicts,
    find_insert_conflicts)
from dbsync.client.net import get_request


class OperationError(Exception): pass


def perform(operation, content_types, container, session):
    """Performs *operation*, looking for required data and metadata in
    *content_types* and *container*, and using *session* to perform
    it.

    *container* is an instance of dbsync.messages.base.BaseMessage.

    If at any moment this operation fails for predictable causes, it
    will raise an *OperationError*."""
    ct = lookup(attr("content_type_id") == operation.content_type_id,
                content_types)
    if ct is None:
        raise OperationError("no content type for this operation", operation)
    model = lookup(attr("__name__") == ct.model_name,
                   core.synched_models.itervalues())
    if model is None:
        raise OperationError("no model for this operation", operation)

    if operation.command == 'i':
        objs = container.query(model).\
            filter(attr("__pk__") == operation.row_id).all()
        if not objs:
            raise OperationError(
                "no object backing the operation in container", operation)
        obj = objs[0]
        session.add(obj)
        session.flush()

    elif operation.command == 'u':
        obj = query_model(session, model).\
            filter(getattr(model, get_pk(model)) == operation.row_id).first()
        if obj is None:
            raise OperationError(
                "the referenced object doesn't exist in database", operation)
        pull_objs = container.query(model).\
            filter(attr("__pk__") == operation.row_id).all()
        if not pull_objs:
            raise OperationError(
                "no object backing the operation in container", operation)
        pull_obj = pull_objs[0]
        for k, v in properties_dict(pull_obj):
            setattr(obj, k, v)
        session.flush()

    elif operation.command == 'd':
        obj = query_model(session, model).\
            filter(getattr(model, get_pk(model)) == operation.row_id).first()
        if obj is None:
            raise OperationError(
                "the referenced object doesn't exist in database", operation)
        session.delete(obj)
        session.flush()

    else:
        raise OperationError(
            "the operation doesn't specify a valid command ('i', 'u', 'd')",
            operation)


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
            perform(pull_op, content_types, pull_message, session)

    # second phase: insert versions from the pull_message
    for pull_version in pull_message.versions:
        session.add(pull_version)


class BadResponseError(Exception): pass


def pull(pull_url):
    """Attempts a pull from the server.

    The pull operation handling should be configured with specialized
    listeners given by the programmer.

    If not interrupted, the pull will perform a local merge. If the
    response from the server isn't appropriate, it will raise a
    dbysnc.client.pull.BadResponseError."""
    assert isinstance(pull_url, basestring), "pull url must be a string"
    assert bool(pull_url), "pull url can't be empty"
    code, reason, response = get_request(
        pull_url, {'latest_version_id': core.get_latest_version_id()})
    if (code // 100 != 2) or response is None:
        raise BadResponseError(code, reason, response)
    message = None
    try:
        message = PullMessage(response)
    except KeyError:
        raise BadResponseError(
            "response object isn't a valid PullMessage", response)
    merge(message)
