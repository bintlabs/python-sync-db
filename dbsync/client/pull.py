"""
Pull, merge and related operations.
"""

from dbsync.lang import *
from dbsync.core import Session
from dbsync.messages.pull import PullMessage
from dbsync.client.conflicts import resolve_conflicts


def merge(pull_message):
    """Merges a message from the server with the local database.

    *pull_message* is an instance of dbsync.messages.pull.PullMessage."""
    if not isinstance(pull_message, PullMessage):
        raise TypeError("need an instance of dbsync.messages.pull.PullMessage "\
                            "to perform the local merge operation")
    # preamble: detect conflicts between pulled operations and unversioned ones
    session = Session()
    resolve_conflicts(pull_message, session)
    # merge transaction
    # first phase: move the local operations and objects out of the way
    # TODO first phase
