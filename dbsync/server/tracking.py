"""
Listeners to SQLAlchemy events to keep track of CUD operations.

On the server side, each operation will also trigger a new version, so
as to allow direct use of the database while maintaining occassionally
connected nodes capable of synchronizing their data.
"""

import logging
import datetime

from sqlalchemy import event

from dbsync import core
from dbsync.models import Operation, ContentType, Version


def make_listener(command):
    """Builds a listener for the given command (i, u, d)."""
    def listener(mapper, connection, target):
        if not core.listening: return
        session = core.Session()
        tname = mapper.mapped_table.name
        ct = session.query(ContentType).\
            filter(ContentType.table_name == tname).first()
        if ct is None:
            logging.error("you must register a content type for {0} "\
                              "to keep track of operations".format(tname))
            return
        # one version for each operation
        version = Version(created=datetime.datetime.now())
        pk = getattr(target, mapper.primary_key[0].name)
        op = Operation(
            row_id=pk,
            content_type_id=ct.content_type_id,
            command=command)
        session.add(version)
        session.add(op)
        op.version = version
        session.commit()
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
