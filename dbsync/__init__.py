"""
Top-level exports, for convenience.
"""

import dbsync.core
from dbsync.core import (
    is_synched,
    generate_content_types,
    set_engine,
    get_engine,
    save_extensions)
from dbsync.models import Base
from dbsync.logs import set_log_target


__version_info__ = (0, 7, 0)
__version__ = '.'.join(str(n) for n in __version_info__)


def create_all():
    "Issues DDL commands."
    Base.metadata.create_all(get_engine())


def drop_all():
    "Issues DROP commands."
    Base.metadata.drop_all(get_engine())


def set_listening_mutex(_):
    "DEPRECATED"
    pass
