"""
Utility functions.
"""

import random
from sqlalchemy.orm import object_mapper, class_mapper, ColumnProperty


def generate_secret(length=128):
    chars = "0123456789"\
        "abcdefghijklmnopqrstuvwxyz"\
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"\
        ".,_-+*@:;[](){}~!?'|<>=/\&%$#"
    return "".join(random.choice(chars) for _ in xrange(length))


def properties_dict(sa_object):
    """Returns a dictionary of column-properties for the given
    SQLAlchemy mapped object."""
    mapper = object_mapper(sa_object)
    return dict((prop.key, getattr(sa_object, prop.key))
                for prop in mapper.iterate_properties
                if isinstance(prop, ColumnProperty))


def types_dict(sa_class):
    """Returns a dictionary of column-properties mapped to their
    SQLAlchemy types for the given mapped class."""
    mapper = class_mapper(sa_class)
    return dict((prop.key, prop.columns[0].type)
                for prop in mapper.iterate_properties
                if isinstance(prop, ColumnProperty))


def object_from_dict(class_, dict_):
    """Returns an object from a dictionary of attributes."""
    obj = class_()
    for k in dict_:
        setattr(obj, k, dict_[k])
    return obj


def get_pk(sa_class):
    """Returns the primary key name for the given mapped class."""
    mapper = class_mapper(sa_class)
    return mapper.primary_key[0].key
