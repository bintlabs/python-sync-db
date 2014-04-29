"""
Listeners to SQLAlchemy events to keep track of CUD operations.
"""

import logging
from collections import deque

from sqlalchemy import event
from sqlalchemy.orm.session import Session as GlobalSession

from dbsync import core
from dbsync.models import Operation, ContentType


#: Operations to be flushed to the database after a commit.
_operations_queue = deque()


def flush_operations(_):
    """Flush operations after a commit has been issued."""
    if not _operations_queue or not core.listening: return
    session = core.Session()
    while _operations_queue:
        op = _operations_queue.popleft()
        session.add(op)
        session.flush()
    session.commit()
    session.close()


def empty_queue(*_):
    """Empty the operations queue."""
    if not core.listening: return
    while _operations_queue:
        _operations_queue.pop()


def make_listener(command):
    """Builds a listener for the given command (i, u, d)."""
    def listener(mapper, connection, target):
        if not core.listening: return
        if command == 'u' and not core.SessionClass.object_session(target).\
                is_modified(target, include_collections=False):
            return
        session = core.Session()
        tname = mapper.mapped_table.name
        ct = session.query(ContentType).\
            filter(ContentType.table_name == tname).first()
        if ct is None:
            logging.error("you must register a content type for {0} "\
                              "to keep track of operations".format(tname))
            return
        pk = getattr(target, mapper.primary_key[0].name)
        op = Operation(
            row_id=pk,
            version_id=None, # operation not yet versioned
            content_type_id=ct.content_type_id,
            command=command)
        _operations_queue.append(op)
        session.close()
    return listener


def track(model):
    """Adds an ORM class to the list of synchronized classes.

    It can be used as a class decorator. This will also install
    listeners to keep track of CUD operations for the given model."""
    if model.__name__ in core.synched_models:
        return model
    core.synched_models[model.__name__] = model
    event.listen(model, 'after_insert', make_listener('i'))
    event.listen(model, 'after_update', make_listener('u'))
    event.listen(model, 'after_delete', make_listener('d'))
    return model


event.listen(GlobalSession, 'after_commit', flush_operations)
event.listen(GlobalSession, 'after_soft_rollback', empty_queue)
