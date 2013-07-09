"""
Pull, merge and related operations.
"""

from dbsync.core import Session
from dbsync.models import Operation
from dbsync.messages.pull import compressed_operations


def merge(pull_message):
    """Merges a message from the server with the local database.

    *pull_message* is an instance of dbsync.messages.pull.PullMessage."""
    # preamble: detect conflicts between pulled operations and unversioned ones
    session = Session()
    unversioned_ops = session.query(Operation).\
        filter(Operation.version_id == None).order_by(Operation.order.asc())
    pull_ops = compressed_operations(pull_message.operations)
    conflicts = [(pull_op, local_op)
                 for pull_op in pull_ops
                 if pull_op.command == 'u' or pull_op.command == 'd'
                 for local_op in unversioned_ops
                 if local_op.command == 'u' or local_op.command == 'd'
                 if pull_op.row_id == local_op.row_id
                 if pull_op.content_type_id == local_op.content_type_id]
    # merge transaction
    # first phase: move the local operations and objects out of the way
    # TODO first phase
