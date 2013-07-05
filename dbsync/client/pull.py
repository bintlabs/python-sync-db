"""
Pull, merge and related operations.
"""

from dbsync.core import Session


def merge(pull_message):
    """Merges a message from the server with the local database.

    *pull_message* is an instace of dbsync.messages.pull.PullMessage."""
    # TODO merge and conflict resolution
    pass
