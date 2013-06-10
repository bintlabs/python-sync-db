"""
Internal model used to keep track of versions and operations.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from dbsync.settings import declarative_base_class as Base, prefix

if Base is None:
    raise ImportError(
        "Can't import synchronization models before setting the declarative "\
            "base class. Use dbsync.set_declarative_base_class(class_) to set "\
            "one, or give no arguments to set the default one.")


class ContentType(Base):
    """A weak abstraction over a database table."""

    __tablename__ = prefix + "content_types"

    content_type_id = Column(Integer, primary_key=True)
    table_name = Column(String)


class Version(Base):
    """A database version. These are added for each 'push' accepted
    and executed without problems."""

    __tablename__ = prefix + "versions"

    version_id = Column(Integer, primary_key=True)
    created = Column(DateTime)


class Operation(Base):
    """A database operation (insert, delete or update).

    The operations are grouped in versions and ordered as they are
    executed.
    """

    __tablename__ = prefix + "operations"

    row_id = Column(Integer)
    version_id = Column(
        Integer,
        ForeignKey(Version.__tablename__ + ".version_id"),
        nullable=True)
    content_type_id = Column(
        Integer, ForeignKey(Version.__tablename__ + ".content_type_id"))
    command = Column(String(1))
    order = Column(Integer, primary_key=True)

    version = relationship(Version, backref="operations")
    content_type = relationship(ContentType, backref="operations")


class Node(Base):
    """A node registry.

    A node is a client application installed somewhere else."""

    __tablename__ = prefix + "nodes"

    node_id = Column(Integer, primary_key=True)
    registered = Column(DateTime)
    registry_user_id = Column(Integer)
    secret = Column(String(128))
