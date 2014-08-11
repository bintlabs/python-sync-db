"""
Listeners to SQLAlchemy events to keep track of CUD operations.
"""

import logging
import inspect
from collections import deque

from sqlalchemy import event
from sqlalchemy.orm.session import Session as GlobalSession

from dbsync import core
from dbsync.models import Operation, ContentType


#: Operations to be flushed to the database after a commit.
_operations_queue = deque()


def flush_operations(_):
    "Flush operations after a commit has been issued."
    if not _operations_queue or not core.listening: return
    session = core.Session()
    while _operations_queue:
        op = _operations_queue.popleft()
        session.add(op)
        session.flush()
    session.commit()
    session.close()


def empty_queue(*_):
    "Empty the operations queue."
    if not core.listening: return
    while _operations_queue:
        _operations_queue.pop()


#: table name => content_type id
cts_cache = {}

def get_ct(tname, session):
    in_cache = cts_cache.get(tname, None)
    if in_cache is None:
        ct = session.query(ContentType).\
            filter(ContentType.table_name == tname).first()
        if ct is not None:
            cts_cache[tname] = ct.content_type_id
            return ct.content_type_id
    return in_cache


def make_listener(command):
    "Builds a listener for the given command (i, u, d)."
    def listener(mapper, connection, target):
        if not core.listening: return
        if command == 'u' and not core.SessionClass.object_session(target).\
                is_modified(target, include_collections=False):
            return
        session = core.Session()
        tname = mapper.mapped_table.name
        ct = get_ct(tname, session)
        if ct is None:
            logging.error("you must register a content type for {0} "\
                              "to keep track of operations".format(tname))
            return
        pk = getattr(target, mapper.primary_key[0].name)
        op = Operation(
            row_id=pk,
            version_id=None, # operation not yet versioned
            content_type_id=ct,
            command=command)
        _operations_queue.append(op)
        session.close()
    return listener


def _start_tracking(model, directions):
    if 'pull' in directions:
        core.pulled_models.add(model)
    if 'push' in directions:
        core.pushed_models.add(model)
    if model.__name__ in core.synched_models:
        return model
    core.synched_models[model.__name__] = model
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
