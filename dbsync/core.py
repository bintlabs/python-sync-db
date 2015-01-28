"""
Common functionality for model synchronization and version tracking.
"""

import zlib
import inspect
import logging
logging.getLogger('dbsync').addHandler(logging.NullHandler())

from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine

from dbsync.lang import *
from dbsync.utils import get_pk, query_model, copy, QueryWrapper
from dbsync.models import Operation, Version
from dbsync import dialects
from dbsync.logs import get_logger


logger = get_logger(__name__)


INTERNAL_SESSION_ATTR = '_dbsync_internal'
INTERNAL_OBJECT_ATTR = '_dbsync_internal'


SessionClass = sessionmaker(autoflush=False, expire_on_commit=False)
def Session():
    s = SessionClass(bind=get_engine())
    s._model_changes = dict() # for flask-sqlalchemy
    setattr(s, INTERNAL_SESSION_ATTR, True) # used to disable listeners
    return s


@guard
def process_object(obj):
    setattr(obj, INTERNAL_OBJECT_ATTR, True)
    return obj
QueryWrapper.process = staticmethod(process_object)


#: The internal use mode, used to prevent client-server module
#  collision. Possible values are 'modeless', 'client', 'server'.
mode = 'modeless'


#: The engine used for database connections.
_engine = None


def set_engine(engine):
    """
    Sets the SA engine to be used by the library.

    It should point to the same database as the application's.
    """
    assert isinstance(engine, Engine), "expected sqlalchemy.engine.Engine object"
    global _engine
    _engine = engine


class ConfigurationError(Exception): pass

def get_engine():
    "Returns a defined (not None) engine."
    if _engine is None:
        raise ConfigurationError("database engine hasn't been set yet")
    return _engine


class tracked_record(object):

    def __setattr__(self, *args):
        raise AttributeError("'tracked_record' object is immutable")
    __delattr__ = __setattr__

    def __init__(self, model=None, id=None):
        super(tracked_record, self).__setattr__('model', model)
        super(tracked_record, self).__setattr__('id', id)

null_model = tracked_record()

def install(self, model):
    """
    Installs the model in synched_models, indexing by class, class
    name, table name and content_type_id.
    """
    ct_id = make_content_type_id(model)
    tname = model.__table__.name
    record = tracked_record(model=model, id=ct_id)
    self.model_names[model.__name__] = record
    self.models[model] = record
    self.tables[tname] = record
    self.ids[ct_id] = record

#: Set of classes marked for synchronization and change tracking.
synched_models = type(
    'synched_models',
    (object,),
    {'tables': dict(),
     'models': dict(),
     'model_names': dict(),
     'ids': dict(),
     'install': install})()


def tracked_model(operation):
    "Get's the tracked model (SA mapped class) for this operation."
    return synched_models.ids.get(operation.content_type_id, null_model).model
# Injects synched models lookup into the Operation class.
Operation.tracked_model = property(tracked_model)


#: Set of classes in *synched_models* that are subject to pull handling.
pulled_models = set()


#: Set of classes in *synched_models* that are subject to push handling.
pushed_models = set()


#: Extensions to tracked models.
model_extensions = {}


def extend(model, fieldname, fieldtype, loadfn, savefn, deletefn=None):
    """
    Extends *model* with a field of name *fieldname* and type
    *fieldtype*.

    *fieldtype* should be an instance of a SQLAlchemy type class, or
    the class itself.

    *loadfn* is a function called to populate the extension. It should
    accept an instance of the model and should return the value of the
    field.

    *savefn* is a function called to persist the field. It should
    accept the instance of the model and the field's value. It's
    return value is ignored.

    *deletefn* is a function called to revert the side effects of
    *savefn* for old values. It gets called after an update on the
    object with the old object's values, or after a delete. *deletefn*
    is optional, and if given it should be a function of two
    arguments: the first is the object in the previous state, the
    second is the object in the current state.

    Original proposal: https://gist.github.com/kklingenberg/7336576
    """
    assert inspect.isclass(model), "model must be a mapped class"
    assert isinstance(fieldname, basestring) and bool(fieldname),\
        "field name must be a non-empty string"
    assert not hasattr(model, fieldname),\
        "the model {0} already has the attribute {1}".\
        format(model.__name__, fieldname)
    assert inspect.isroutine(loadfn), "load function must be a callable"
    assert inspect.isroutine(savefn), "save function must be a callable"
    assert deletefn is None or inspect.isroutine(deletefn),\
        "delete function must be a callable"
    extensions = model_extensions.get(model.__name__, {})
    type_ = fieldtype if not inspect.isclass(fieldtype) else fieldtype()
    extensions[fieldname] = (type_, loadfn, savefn, deletefn)
    model_extensions[model.__name__] = extensions


def _has_extensions(obj):
    return bool(model_extensions.get(type(obj).__name__, {}))

def _has_delete_functions(obj):
    return any(
        delfn is not None
        for t, loadfn, savefn, delfn in model_extensions.get(
            type(obj).__name__, {}).itervalues())


def save_extensions(obj):
    """
    Executes the save procedures for the extensions of the given
    object.
    """
    extensions = model_extensions.get(type(obj).__name__, {})
    for field, ext in extensions.iteritems():
        _, _, savefn, _ = ext
        try: savefn(obj, getattr(obj, field, None))
        except:
            logger.exception(
                u"Couldn't save extension %s for object %s", field, obj)


def delete_extensions(old_obj, new_obj):
    """
    Executes the delete procedures for the extensions of the given
    object. *old_obj* is the object in the previous state, and
    *new_obj* is the object in the current state (or ``None`` if the
    object was deleted).
    """
    extensions = model_extensions.get(type(old_obj).__name__, {})
    for field, ext in extensions.iteritems():
        _, _, _, deletefn = ext
        if deletefn is not None:
            try: deletefn(old_obj, new_obj)
            except:
                logger.exception(
                    u"Couldn't delete extension %s for object %s", field, new_obj)


#: Toggled variable used to disable listening to operations momentarily.
listening = True


def toggle_listening(enabled=None):
    """
    Change the listening state.

    If set to ``False``, no operations will be registered. This can be
    used to disable dbsync temporarily, in scripts or blocks that
    execute in a single-threaded environment.
    """
    global listening
    listening = enabled if enabled is not None else not listening
    if not listening:
        logger.warning("dbsync is disabled; listening is set to False")


def with_listening(enabled):
    """
    Decorator for procedures to be executed with the specified
    listening status.
    """
    def wrapper(proc):
        @wraps(proc)
        def wrapped(*args, **kwargs):
            prev = bool(listening)
            toggle_listening(enabled)
            try:
                return proc(*args, **kwargs)
            finally:
                toggle_listening(prev)
        return wrapped
    return wrapper


# Helper functions used to queue extension operations in a transaction.

def _track_added(fn, added):
    def tracked(o, **kws):
        if _has_extensions(o): added.append(o)
        return fn(o, **kws)
    return tracked

def _track_deleted(fn, deleted, session, always=False):
    def tracked(o, **kws):
        if _has_delete_functions(o):
            if always: deleted.append((copy(o), None))
            else:
                prev = query_model(session, type(o)).filter_by(
                    **{get_pk(o): getattr(o, get_pk(o), None)}).\
                    first()
                if prev is not None:
                    deleted.append((copy(prev), o))
        return fn(o, **kws)
    return tracked


def with_transaction(include_extensions=True):
    """
    Decorator for a procedure that uses a session and acts as an
    atomic transaction. It feeds a new session to the procedure, and
    commits it, rolls it back, and / or closes it when it's
    appropriate. If *include_extensions* is ``False``, the transaction
    will ignore model extensions.
    """
    def wrapper(proc):
        @wraps(proc)
        def wrapped(*args, **kwargs):
            extensions = kwargs.pop('include_extensions',
                                    include_extensions)
            session = Session()
            previous_state = dialects.begin_transaction(session)
            added = []
            deleted = []
            if extensions:
                session.add = _track_deleted(
                    _track_added(session.add, added),
                    deleted,
                    session)
                session.merge = _track_deleted(
                    _track_added(session.merge, added),
                    deleted,
                    session)
                session.delete = _track_deleted(
                    session.delete,
                    deleted,
                    session,
                    always=True)
            result = None
            try:
                kwargs.update({'session': session})
                result = proc(*args, **kwargs)
                session.commit()
            except:
                session.rollback()
                raise
            finally:
                dialects.end_transaction(previous_state, session)
                session.close()
            for old_obj, new_obj in deleted: delete_extensions(old_obj, new_obj)
            for obj in added: save_extensions(obj)
            return result
        return wrapped
    return wrapper


def make_content_type_id(model):
    "Returns a content type id for the given model."
    mname = model.__name__
    tname = model.__table__.name
    return zlib.crc32("{0}/{1}".format(mname, tname), 0) & 0xffffffff


def generate_content_types():
    """
    Fills the content type table.

    Inserts content types into the internal table used to describe
    operations.

    DEPRECATED since ContentType was removed.
    """
    logger.warning("content types no longer live in database")
    logger.warning("in-memory content_type_ids:\n{0}".format(
            "\n".join("{0} :\t{1}".format(ct_id, record.model.__name__)
                      for ct_id, record in synched_models.ids.iteritems())))


def is_synched(obj):
    """
    Returns whether the given tracked object is synched.

    Raises a TypeError if the given object is not being tracked
    (i.e. the content type doesn't exist).
    """
    if type(obj) not in synched_models.models:
        raise TypeError("the given object of class {0} isn't being tracked".\
                            format(obj.__class__.__name__))
    session = Session()
    last_op = session.query(Operation).\
        filter(Operation.content_type_id == synched_models.models[type(obj)].id,
               Operation.row_id == getattr(obj, get_pk(obj))).\
               order_by(Operation.order.desc()).first()
    result = last_op is None or last_op.version_id is not None
    session.close()
    return result


def get_latest_version_id(session=None):
    """
    Returns the latest version identifier or ``None`` if no version is
    found.
    """
    closeit = session is None
    session = Session() if closeit else session
    # assuming version identifiers grow monotonically
    # might need to order by 'created' datetime field
    version = session.query(Version).order_by(Version.version_id.desc()).first()
    if closeit:
        session.close()
    return maybe(version, attr('version_id'), None)
