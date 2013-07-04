"""
Common functionality for model synchronization and version tracking.
"""

from sqlalchemy.orm import sessionmaker

from dbsync.lang import *
from dbsync.utils import get_pk
from dbsync.models import ContentType, Operation


_SessionClass = sessionmaker()
Session = lambda: _SessionClass(bind=get_engine())


#: The engine used for database connections.
_engine = None


def set_engine(engine):
    """Sets the SA engine to be used by the library.

    It should point to the same database as the application's."""
    global _engine
    _engine = engine


def get_engine():
    """Returns a defined (not None) engine."""
    if _engine is None:
        raise ValueError("database engine hasn't been set yet")
    return _engine


#: Set of classes marked for synchronization and change tracking.
synched_models = {}


#: Toggled variable used to disable listening to operations momentarily.
listening = True


def toggle_listening(enabled=None):
    """Change the listening state.

    If set to ``False``, no operations will be registered. This is
    used in the conflict resolution phase."""
    global listening
    listening = enabled if enabled is not None else not listening


def generate_content_types():
    """Fills the content type table.

    Inserts content types into the internal table used to describe
    operations."""
    session = Session()
    for _, model in sorted(synched_models.items(), key=fst):
        tname = model.__table__.name
        if session.query(ContentType).\
                filter(ContentType.table_name == tname).count() == 0:
            session.add(ContentType(table_name=tname, model_name=model.__name__))
    session.commit()


def is_synched(obj):
    """Returns whether the given tracked object is synched.

    Raises a TypeError if the given object is not being tracked
    (i.e. the content type doesn't exist)."""
    session = Session()
    ct = session.query(ContentType).\
        filter(ContentType.model_name == obj.__class__.__name__).first()
    if ct is None:
        raise TypeError("the given object of class {0} isn't being tracked".\
                            format(obj.__class__.__name__))
    pk_name = get_pk(obj)
    last_op = session.query(Operation).\
        filter(Operation.content_type_id == ct.content_type_id,
               Operation.row_id == getattr(obj, pk_name)).\
               order_by(Operation.order.desc()).first()
    if last_op is None:
        return True
    return last_op.version_id is not None
