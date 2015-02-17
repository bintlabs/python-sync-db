"""
Trim the server synchronization tables to free space.
"""

from dbsync.lang import *
from dbsync import core
from dbsync.models import Node, Version, Operation


@core.session_committing
def trim(session=None):
    """
    Clears space by deleting operations and versions that are no
    longer needed.

    This might cause the server to answer incorrectly to pull requests
    from nodes that were late to register. To go around that, a repair
    should be enforced after the node's register.

    Another problem with this procedure is that it won't clear space
    if there's at least one abandoned node registered. The task of
    keeping the nodes registry clean of those is left to the
    programmer.
    """
    versions = [maybe(session.query(Version).\
                          filter(Version.node_id == node.node_id).\
                          order_by(Version.version_id.desc()).first(),
                      attr('version_id'),
                      None)
                for node in session.query(Node)]
    if not versions:
        last_id = core.get_latest_version_id(session=session)
        # all operations are versioned according to dbsync.server.track
        session.query(Operation).delete()
        session.query(Version).filter(Version.version_id != last_id).delete()
        return
    if None in versions: return # dead nodes block the trim
    minversion = min(versions)
    session.query(Operation).filter(Operation.version_id <= minversion).delete()
    session.query(Version).filter(Version.version_id < minversion).delete()
