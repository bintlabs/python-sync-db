"""
Common functionality for model synchronization and version tracking.
"""

import zlib

from sqlalchemy.orm import sessionmaker

from dbsync.lang import *
from dbsync.utils import get_pk
from dbsync.models import ContentType, Operation, Version


_SessionClass = sessionmaker(autoflush=False)
def Session():
    s = _SessionClass(bind=get_engine())
    s._model_changes = dict() # for flask-sqlalchemy
    return s


#: The engine used for database connections.
_engine = None


def set_engine(engine):
    """Sets the SA engine to be used by the library.

    It should point to the same database as the application's."""
    global _engine
    _engine = engine


class ConfigurationError(Exception): pass

def get_engine():
    """Returns a defined (not None) engine."""
    if _engine is None:
        raise ConfigurationError("database engine hasn't been set yet")
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


def with_listening(enabled):
    """Decorator for procedures to be executed with the specified
    listening status."""
    def wrapper(proc):
        @wraps(proc)
        def wrapped(*args, **kwargs):
            prev = listening
            toggle_listening(enabled)
            try:
                result = proc(*args, **kwargs)
                toggle_listening(prev)
                return result
            except:
                toggle_listening(prev)
                raise
        return wrapped
    return wrapper


def with_transaction(proc):
    """Decorator for a procedure that uses a session and acts as an
    atomic transaction. It feeds a new session to the procedure, and
    commits it, rolls it back, and / or closes it when it's
    appropriate."""
    @wraps(proc)
    def wrapped(*args, **kwargs):
        session = Session()
        try:
            kwargs.update({"session": session})
            result = proc(*args, **kwargs)
            session.commit()
            session.close()
            return result
        except:
            session.rollback()
            session.close()
            raise
    return wrapped


def generate_content_types():
    """Fills the content type table.

    Inserts content types into the internal table used to describe
    operations."""
    session = Session()
    for mname, model in synched_models.iteritems():
        tname = model.__table__.name
        content_type_id = zlib.crc32('{0}/{1}'.format(mname, tname), 0) \
            & 0xffffffff
        if session.query(ContentType).\
                filter(ContentType.table_name == tname).count() == 0:
            session.add(ContentType(table_name=tname,
                                    model_name=mname,
                                    content_type_id=content_type_id))
    session.commit()
    session.close()


def is_synched(obj):
    """Returns whether the given tracked object is synched.

    Raises a TypeError if the given object is not being tracked
    (i.e. the content type doesn't exist)."""
    session = Session()
    ct = session.query(ContentType).\
        filter(ContentType.model_name == obj.__class__.__name__).first()
    if ct is None:
        session.close()
        raise TypeError("the given object of class {0} isn't being tracked".\
                            format(obj.__class__.__name__))
    last_op = session.query(Operation).\
        filter(Operation.content_type_id == ct.content_type_id,
               Operation.row_id == getattr(obj, get_pk(obj))).\
               order_by(Operation.order.desc()).first()
    result = last_op is None or last_op.version_id is not None
    session.close()
    return result


def get_latest_version_id(session=None):
    """Returns the latest version identifier or ``None`` if no version
    is found."""
    closeit = session is None
    session = Session() if closeit else session
    # assuming version identifiers grow monotonically
    # might need to order by 'created' datetime field
    version = session.query(Version).order_by(Version.version_id.desc()).first()
    if closeit:
        session.close()
    return maybe(version, attr("version_id"), None)
