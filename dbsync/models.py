"""
Internal model used to keep track of versions and operations.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship, backref, validates
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta


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


class Version(Base):
    """A database version.

    These are added for each 'push' accepted and executed without
    problems."""

    __tablename__ = "versions"

    version_id = Column(Integer, primary_key=True)
    created = Column(DateTime)

    def __repr__(self):
        return u"<Version version_id: {0}, created: {1}>".\
            format(self.version_id, self.created)


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
    content_type = relationship(
        ContentType, backref=backref("operations", lazy="joined"))

    @validates('command')
    def validate_command(self, key, command):
        assert command in self.command_options
        return command

    def __repr__(self):
        return u"<Operation row_id: {0}, content_type_id: {1}, command: {2}>".\
            format(self.row_id, self.content_type_id, self.command)


class Node(Base):
    """A node registry.

    A node is a client application installed somewhere else."""

    __tablename__ = "nodes"

    node_id = Column(Integer, primary_key=True)
    registered = Column(DateTime)
    registry_user_id = Column(Integer)
    secret = Column(String(128))
