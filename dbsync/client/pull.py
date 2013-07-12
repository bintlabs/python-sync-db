"""
Pull, merge and related operations.
"""

from dbsync.lang import *
from dbsync import core
from dbsync.messages.pull import PullMessage
from dbsync.client.compression import compress, compressed_operations
from dbsync.client.conflicts import (
    find_direct_conflicts,
    find_dependency_conflicts)


@core.with_listening(False)
def merge(pull_message):
    """Merges a message from the server with the local database.

    *pull_message* is an instance of dbsync.messages.pull.PullMessage."""
    if not isinstance(pull_message, PullMessage):
        raise TypeError("need an instance of dbsync.messages.pull.PullMessage "\
                            "to perform the local merge operation")
    session = core.Session()
    content_types = session.query(ContentType).all()
    # preamble: detect conflicts between pulled operations and unversioned ones
    compress()
    unversioned_ops = session.query(Operation).\
        filter(Operation.version_id == None).order_by(Operation.order.asc())
    pull_ops = compressed_operations(pull_message.operations)

    direct_conflicts = find_direct_conflicts(unversioned_ops, pull_ops)

    # in which the delete operation is registered on the pull message
    dependency_conflicts_pull = find_dependency_conflicts(
        unversioned_ops, pull_ops, content_types, session)

    # in which the delete operation was performed locally
    dependency_conflicts_local = map(swap, find_dependency_conflicts(
        pull_ops, unversioned_ops, content_types, session))

    # merge transaction
    # first phase: move the local operations and objects out of the way
    # TODO first phase
