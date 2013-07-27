"""
Utility functions.
"""

import random
import inspect
from sqlalchemy.orm import (
    object_mapper,
    class_mapper,
    ColumnProperty,
    noload)


def generate_secret(length=128):
    chars = "0123456789"\
        "abcdefghijklmnopqrstuvwxyz"\
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"\
        ".,_-+*@:;[](){}~!?|<>=/\&%$#"
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


def parent_objects(sa_object, models, session):
    """Returns all the parent objects the given *sa_object* point to
    (through foreign keys in *sa_object*).

    *models* is a list of mapped classes.

    *session* must be a valid SA session instance."""
    mapper = object_mapper(sa_object)
    references = [(getattr(sa_object, k.parent.name), k.column.table)
                  for k in mapper.mapped_table.foreign_keys]
    def get_model(table):
        for m in models:
            if class_mapper(m).mapped_table == table:
                return m
        return None
    return filter(lambda obj: obj is not None,
                  (query_model(session, m).filter_by(**{get_pk(m): val}).first()
                   for val, m in ((v, get_model(table))
                                  for v, table in references)
                   if m is not None))


def query_model(session, sa_class):
    """Returns a query for *sa_class* that doesn't load any
    relationship attribute."""
    return session.query(sa_class).options(noload('*'))
