"""
Top-level exports, for convenience.
"""

from dbsync.core import (
    is_synched,
    generate_content_types,
    set_engine,
    get_engine)
from dbsync.models import Base
from dbsync import client
from dbsync import server


def create_all():
    """Issues DDL commands."""
    Base.metadata.create_all(get_engine())
