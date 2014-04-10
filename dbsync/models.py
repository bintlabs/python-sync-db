"""
Internal model used to keep track of versions and operations.
"""
import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, BigInteger
from sqlalchemy.orm import relationship, backref, validates
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta

from dbsync.lang import *
from dbsync.utils import get_pk, query_model
import dbsync.core


#: Database tables prefix.
tablename_prefix = "sync_"


class PrefixTables(DeclarativeMeta):
    def __init__(cls, classname, bases, dict_):
        if '__tablename__' in dict_:
            tn = dict_['__tablename__']
            cls.__tablename__ = dict_['__tablename__'] = tablename_prefix + tn
        return super(PrefixTables, cls).__init__(classname, bases, dict_)

Base = declarative_base(metaclass=PrefixTables)


class ContentType(Base):
    """A weak abstraction over a database table."""

    __tablename__ = "content_types"

    content_type_id = Column(BigInteger, primary_key=True)
    table_name = Column(String(500))
    model_name = Column(String(500))

    def __repr__(self):
        return u"<ContentType id: {0}, table_name: {1}, model_name: {2}>".\
            format(self.content_type_id, self.table_name, self.model_name)


class Node(Base):
    """A node registry.

    A node is a client application installed somewhere else."""

    __tablename__ = "nodes"

    node_id = Column(Integer, primary_key=True)
    registered = Column(DateTime)
    registry_user_id = Column(Integer)
    secret = Column(String(128))

    def __init__(self, *args, **kwargs):
        super(Node, self).__init__(*args, **kwargs)

    def __repr__(self):
        return u"<Node node_id: {0}, registered: {1}, "\
            u"registry_user_id: {2}, secret: {3}>".\
            format(self.node_id,
                   self.registered,
                   self.registry_user_id,
                   self.secret)


class Version(Base):
    """A database version.

    These are added for each 'push' accepted and executed without
    problems."""

    __tablename__ = "versions"

    version_id = Column(Integer, primary_key=True)
    node_id = Column(Integer, ForeignKey(Node.__tablename__ + ".node_id"))
    created = Column(DateTime)

    node = relationship(Node)

    def __repr__(self):
        return u"<Version version_id: {0}, created: {1}>".\
            format(self.version_id, self.created)


class OperationError(Exception): pass


class Operation(Base):
    """A database operation (insert, delete or update).

    The operations are grouped in versions and ordered as they are
    executed."""

    __tablename__ = "operations"

    row_id = Column(Integer)
    version_id = Column(
        Integer,
        ForeignKey(Version.__tablename__ + ".version_id"),
        nullable=True)
    content_type_id = Column(
        BigInteger, ForeignKey(ContentType.__tablename__ + ".content_type_id"))
    command = Column(String(1))
    command_options = ('i', 'u', 'd')
    order = Column(Integer, primary_key=True)

    version = relationship(Version, backref=backref("operations", lazy="joined"))
    content_type = relationship(ContentType, backref="operations", lazy="joined")

    @validates('command')
    def validate_command(self, key, command):
        assert command in self.command_options
        return command

    def __repr__(self):
        return u"<Operation row_id: {0}, content_type_id: {1}, command: {2}>".\
            format(self.row_id, self.content_type_id, self.command)

    def perform(operation, content_types, synched_models, container, session, node_id=None):
        """Performs *operation*, looking for required data and
        metadata in *content_types*, *synched_models*, and
        *container*, and using *session* to perform it.

        *container* is an instance of
        dbsync.messages.base.BaseMessage.

        If at any moment this operation fails for predictable causes,
        it will raise an *OperationError*."""
        ct = lookup(attr("content_type_id") == operation.content_type_id,
                    content_types)
        if ct is None:
            raise OperationError("no content type for this operation", operation)
        model = synched_models.get(ct.model_name, None)
        if model is None:
            raise OperationError("no model for this operation", operation)

        if operation.command == 'i':
            objs = container.query(model).\
                filter(attr('__pk__') == operation.row_id).all()
            if not objs:
                raise OperationError(
                    "no object backing the operation in container", operation)
            obj = objs[0]
            session.add(obj)

        elif operation.command == 'u':
            obj = query_model(session, model).\
                filter(getattr(model, get_pk(model)) == operation.row_id).first()
            if obj is None:
                # What should be done in this case !!!
                # For now, the record will be created again, but is an error
                # because nothing should be deleted without using dbsync
                # raise OperationError(
                #     "the referenced object doesn't exist in database", operation)
                dbsync.core.save_log("models.update", node_id,
                    ["the referenced object doesn't exist in database", operation])
                pass

            pull_objs = container.query(model).\
                filter(attr('__pk__') == operation.row_id).all()
            if not pull_objs:
                raise OperationError(
                    "no object backing the operation in container", operation)
            session.merge(pull_objs[0])

        elif operation.command == 'd':
            obj = query_model(session, model, only_pk=True).\
                filter(getattr(model, get_pk(model)) == operation.row_id).first()
            if obj is None:
                # The object is already delete in the server
                # The final state in node and server are the same. But is an error
                # because nothing should be deleted without using dbsync
                # raise OperationError(
                #     "the referenced object doesn't exist in database", "roolback", operation)
                dbsync.core.save_log("models.delete", node_id,
                    ["the referenced object doesn't exist in database", operation])
                pass
            else:
                session.delete(obj)

        else:
            raise OperationError(
                "the operation doesn't specify a valid command ('i', 'u', 'd')",
                operation)


class Log(Base):
    """Error log"""

    __tablename__ = "logs"
    
    id = Column(Integer, primary_key=True)
    created = Column(DateTime)
    source = Column(String(64)) 
    error = Column(String(2048))
    node_id = Column(Integer, ForeignKey(Node.__tablename__ + ".node_id"))

    def __init__(self, *args, **kwargs):
        self.created = datetime.datetime.now()
        super(Log, self).__init__(*args, **kwargs)

    def __repr__(self):
        return u"<Log log_id: {0}, source: {1}, node_id: {2}>".\
            format(self.log_id, self.source, self.node_id)
