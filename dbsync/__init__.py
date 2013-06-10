"""
Initialize module in two steps to allow the inclusion of the tables
used for synchronization in the given schema.
"""

import dbsync.settings as settings
from sqlalchemy.ext.declarative import declarative_base as _base


def set_declarative_base_class(class_=None):
    if class_ is None:
        class_ = _base()
    settings.declarative_base_class = class_
