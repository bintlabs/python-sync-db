"""
Top-level exports, for convenience.
"""

from dbsync.core import (
    is_synched,
    generate_content_types,
    set_engine,
    get_engine)
from dbsync.models import Base


def create_all():
    """Issues DDL commands."""
    Base.metadata.create_all(get_engine())


def drop_all():
    """Issues DROP commands."""
    Base.metadata.drop_all(get_engine())
