"""
Internal model used to keep track of versions and operations.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta

Session = sessionmaker()


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


class Version(Base):
    """A database version.

    These are added for each 'push' accepted and executed without
    problems."""

    __tablename__ = "versions"

    version_id = Column(Integer, primary_key=True)
    created = Column(DateTime)


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
    order = Column(Integer, primary_key=True)

    version = relationship(Version, backref="operations")
    content_type = relationship(ContentType, backref="operations")


class Node(Base):
    """A node registry.

    A node is a client application installed somewhere else."""

    __tablename__ = "nodes"

    node_id = Column(Integer, primary_key=True)
    registered = Column(DateTime)
    registry_user_id = Column(Integer)
    secret = Column(String(128))
