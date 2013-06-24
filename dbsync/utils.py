"""
Utility functions.
"""

import random
from sqlalchemy.orm import object_mapper, ColumnProperty


def generate_secret(length=128):
    chars = "0123456789"\
        "abcdefghijklmnopqrstuvwxyz"\
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"\
        ".,_-+*@:;[](){}~!?'|<>=/\&%$#"
    return "".join(random.choice(chars) for _ in xrange(length))


def properties_dict(sa_object):
    """Returns a dictionary of column-properties for the given
    SQLAlchemy mapped object."""
    mapper = object_mapper(sq_object)
    return dict((prop.key, getattr(sa_object, prop.key))
                for prop in mapper.iterate_properties
                if isinstance(prop, ColumnProperty))
