"""
Common functionality for model synchronization and version tracking.
"""

import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dbsync.models import Base, ContentType


_SessionClass = sessionmaker()
Session = lambda: _SessionClass(bind=get_engine())


#: The engine used for database connections.
_engine = None


def connect_engine(url=None):
    """Creates a new engine for all purposes in this library."""
    global _engine
    if url is None:
        logging.warning(
            "Database engine wasn't connected. "\
                "The model will use an in-memory database to operate.")
        url = "sqlite://"
        _engine = create_engine(url)
        Base.metadata.create_all(_engine)
    else:
        _engine = create_engine(url)


def set_engine(engine):
    """Sets the engine to be used by the library."""
    global _engine
    _engine = engine


def get_engine():
    """Returns a defined (not None) engine.

    If the global engine hasn't been connected yet, it will be the
    default one."""
    if _engine is None:
        connect_engine()
    return _engine


#: List of classes marked for synchronization and change tracking.
synched_models = []


#: Toggled variable used to disable listening to operations momentarily.
listening = True


def toggle_listening(enabled=None):
    """Change the listening state.

    If set to ``False``, no operations will be registered."""
    global listening
    listening = enabled if enabled is not None else not listening


def generate_content_types():
    """Fills the content type table.

    Inserts content types into the internal table used to describe
    operations. *connectable* is a SQLAlchemy Connectable object."""
    session = Session()
    for model in synched_models:
        tname = model.__table__.name
        if session.query(ContentType).\
                filter(ContentType.table_name == tname).count() == 0:
            session.add(ContentType(table_name=tname))
    session.commit()
