"""
Pull, merge and related operations.
"""


def merge(pull_message, node_session):
    """Merges a message from the server with the local database.

    * *pull_message* is an instace of
     dbsync.messages.pull.PullMessage.

    * *node_session* is a valid sqlalchemy session used to access the
     node's database."""
    # TODO merge and conflict resolution
    pass
