"""
Listeners to SQLAlchemy events to keep track of CUD operations.
"""

import logging
import inspect
import warnings
from collections import deque

from sqlalchemy import event
from sqlalchemy.orm.session import Session as GlobalSession

from dbsync import core
from dbsync.models import Operation
from dbsync.logs import get_logger


logger = get_logger(__name__)


if core.mode == 'server':
    warnings.warn("don't import both client and server")
core.mode = 'client'


#: Operations to be flushed to the database after a commit.
_operations_queue = deque()


def flush_operations(committed_session):
    "Flush operations after a commit has been issued."
    if not _operations_queue or \
            getattr(committed_session, core.INTERNAL_SESSION_ATTR, False):
        return
    if not core.listening:
        logger.warning("dbsync is disabled; aborting flush_operations")
        return
    with core.committing_context() as session:
        while _operations_queue:
            op = _operations_queue.popleft()
            session.add(op)
            session.flush()


def empty_queue(*args):
    "Empty the operations queue."
    session = None if not args else args[0]
    if getattr(session, core.INTERNAL_SESSION_ATTR, False):
        return
    if not core.listening:
        logger.warning("dbsync is disabled; aborting empty_queue")
        return
    while _operations_queue:
        _operations_queue.pop()


def make_listener(command):
    "Builds a listener for the given command (i, u, d)."
    def listener(mapper, connection, target):
        if getattr(core.SessionClass.object_session(target),
                   core.INTERNAL_SESSION_ATTR,
                   False):
            return
        if not core.listening:
            logger.warning("dbsync is disabled; "
                           "aborting listener to '{0}' command".format(command))
            return
        if command == 'u' and not core.SessionClass.object_session(target).\
                is_modified(target, include_collections=False):
            return
        tname = mapper.mapped_table.name
        if tname not in core.synched_models.tables:
            logging.error("you must track a mapped class to table {0} "\
                              "to log operations".format(tname))
            return
        pk = getattr(target, mapper.primary_key[0].name)
        op = Operation(
            row_id=pk,
            version_id=None, # operation not yet versioned
            content_type_id=core.synched_models.tables[tname].id,
            command=command)
        _operations_queue.append(op)
    return listener


def _start_tracking(model, directions):
    if 'pull' in directions:
        core.pulled_models.add(model)
    if 'push' in directions:
        core.pushed_models.add(model)
    if model in core.synched_models.models:
        return model
    core.synched_models.install(model)
    if 'push' not in directions:
        return model # don't track operations for pull-only models
    event.listen(model, 'after_insert', make_listener('i'))
    event.listen(model, 'after_update', make_listener('u'))
    event.listen(model, 'after_delete', make_listener('d'))
    return model


def track(*directions):
    """
    Adds an ORM class to the list of synchronized classes.

    It can be used as a class decorator. This will also install
    listeners to keep track of CUD operations for the given model.

    *directions* are optional arguments of values in ('push', 'pull')
    that can restrict the way dbsync handles the class during those
    procedures. If not given, both values are assumed. If only one of
    them is given, the other procedure will ignore the tracked class.
    """
    valid = ('push', 'pull')
    if not directions:
        return lambda model: _start_tracking(model, valid)
    if len(directions) == 1 and inspect.isclass(directions[0]):
        return _start_tracking(directions[0], valid)
    assert all(d in valid for d in directions), \
        "track only accepts the arguments: {0}".format(', '.join(valid))
    return lambda model: _start_tracking(model, directions)


event.listen(GlobalSession, 'after_commit', flush_operations)
event.listen(GlobalSession, 'after_soft_rollback', empty_queue)
