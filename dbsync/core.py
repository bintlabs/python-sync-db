"""
Common functionality for model synchronization and version tracking.
"""

import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dbsync.models import Base, ContentType, Operation


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

    If the global engine hasn't been connected yet, it will create a
    default one."""
    if _engine is None:
        connect_engine()
    return _engine


#: Set of classes marked for synchronization and change tracking.
synched_models = {}


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
    for model in synched_models.values():
        tname = model.__table__.name
        if session.query(ContentType).\
                filter(ContentType.table_name == tname).count() == 0:
            session.add(ContentType(table_name=tname))
    session.commit()


def is_synched(obj):
    """Returns whether the given tracked object is synched.

    Raises a TypeError if the given object is not being tracked
    (i.e. the content type doesn't exist)."""
    session = Session()
    ct = session.query(ContentType).\
        filter(ContentType.table_name == obj.__class__.__table__.name).first()
    if ct is None:
        raise TypeError("the given object of class {0} isn't being tracked".\
                            format(obj.__class__.__name__))
    pk_name = obj.__class__.__mapper__.primary_key[0].name
    last_op = session.query(Operation).\
        filter(Operation.content_type_id == ct.content_type_id,
               Operation.row_id == getattr(obj, pk_name)).\
               order_by(Operation.order.desc()).first()
    if last_op is None:
        return True
    return last_op.version_id is not None
