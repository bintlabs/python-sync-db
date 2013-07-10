"""
Utility functions.
"""

import random
import inspect
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


def get_pk(sa_variant):
    """Returns the primary key name for the given mapped class or
    object."""
    mapper = class_mapper(sa_variant) if inspect.isclass(sa_variant) \
        else object_mapper(sa_variant)
    return mapper.primary_key[0].key


def get_related_tables(sa_variant, models):
    """Returns a list of related SA tables dependent on the given SA
    model or object by foreign key."""
    mapper = class_mapper(sa_variant) if inspect.isclass(sa_variant) \
        else object_mapper(sa_variant)
    return [table for table in (class_mapper(model).mapped_table
                                for model in models)
            if mapper.mapped_table in [key.column.table
                                       for key in table.foreign_keys]]


def get_fks(table_from, table_to):
    """Returns the names of the foreign keys that are defined in
    *table_from* SA table and that refer to *table_to* SA table. If
    the foreign keys don't exist, this procedure returns an empty
    list."""
    fks = filter(lambda k: k.column.table == table_to, table_from.foreign_keys)
    return [fk.parent.name for fk in fks]
