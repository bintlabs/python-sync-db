"""
Internal model used to keep track of versions and operations.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship, backref, validates
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta

from dbsync.lang import *
from dbsync.utils import properties_dict, get_pk, query_model


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

    content_type_id = Column(Integer, primary_key=True)
    table_name = Column(String)
    model_name = Column(String)

    def __repr__(self):
        return u"<ContentType table_name: {0}, model_name: {1}>".\
            format(self.table_name, self.model_name)


class Node(Base):
    """A node registry.

    A node is a client application installed somewhere else."""

    __tablename__ = "nodes"

    node_id = Column(Integer, primary_key=True)
    registered = Column(DateTime)
    registry_user_id = Column(Integer)
    secret = Column(String(128))

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
        Integer, ForeignKey(ContentType.__tablename__ + ".content_type_id"))
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

    def perform(operation, content_types, synched_models, container, session):
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
        model = lookup(attr("__name__") == ct.model_name,
                       synched_models.itervalues())
        if model is None:
            raise OperationError("no model for this operation", operation)

        if operation.command == 'i':
            objs = container.query(model).\
                filter(attr("__pk__") == operation.row_id).all()
            if not objs:
                raise OperationError(
                    "no object backing the operation in container", operation)
            obj = objs[0]
            session.add(obj)
            session.flush()

        elif operation.command == 'u':
            obj = query_model(session, model).\
                filter(getattr(model, get_pk(model)) == operation.row_id).first()
            if obj is None:
                raise OperationError(
                    "the referenced object doesn't exist in database", operation)
            pull_objs = container.query(model).\
                filter(attr("__pk__") == operation.row_id).all()
            if not pull_objs:
                raise OperationError(
                    "no object backing the operation in container", operation)
            pull_obj = pull_objs[0]
            for k, v in properties_dict(pull_obj):
                setattr(obj, k, v)
            session.flush()

        elif operation.command == 'd':
            obj = query_model(session, model).\
                filter(getattr(model, get_pk(model)) == operation.row_id).first()
            if obj is None:
                raise OperationError(
                    "the referenced object doesn't exist in database", operation)
            session.delete(obj)
            session.flush()

        else:
            raise OperationError(
                "the operation doesn't specify a valid command ('i', 'u', 'd')",
                operation)
