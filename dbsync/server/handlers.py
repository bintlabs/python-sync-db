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

from dbsync import core


def handle_pull():
    pass


@core.with_listening(False)
@core.with_transaction
def handle_push(session=None):
    pass
