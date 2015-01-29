"""
Listeners to SQLAlchemy events to keep track of CUD operations.

On the server side, each operation will also trigger a new version, so
as to allow direct use of the database while maintaining occassionally
connected nodes capable of synchronizing their data.
"""

import logging
import inspect
import datetime
import warnings

from sqlalchemy import event

from dbsync import core
from dbsync.models import Operation, Version
from dbsync.logs import get_logger


logger = get_logger(__name__)


if core.mode == 'client':
    warnings.warn("don't import both server and client")
core.mode = 'server'


def make_listener(command):
    "Builds a listener for the given command (i, u, d)."
    @core.session_committing
    def listener(mapper, connection, target, session=None):
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
        # one version for each operation
        version = Version(created=datetime.datetime.now())
        pk = getattr(target, mapper.primary_key[0].name)
        op = Operation(
            row_id=pk,
            content_type_id=core.synched_models.tables[tname].id,
            command=command)
        session.add(version)
        session.add(op)
        op.version = version
    return listener


def _start_tracking(model, directions):
    if 'pull' in directions:
        core.pulled_models.add(model)
    if 'push' in directions:
        core.pushed_models.add(model)
    if model in core.synched_models.models:
        return model
    core.synched_models.install(model)
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
    handlers. If not given, both values are assumed. If only one of
    them is given, the other handler will ignore the tracked class.
    """
    valid = ('push', 'pull')
    if not directions:
        return lambda model: _start_tracking(model, valid)
    if len(directions) == 1 and inspect.isclass(directions[0]):
        return _start_tracking(directions[0], valid)
    assert all(d in valid for d in directions), \
        "track only accepts the arguments: {0}".format(', '.join(valid))
    return lambda model: _start_tracking(model, directions)
