"""
Conflict detection and resolution for the local merge operation.

This module handles the conflict resolution that's required for the
local merge operation.

TODO: Resolve conflicts according to programmer-given listener
procedures and/or model-level directives.
"""

from sqlalchemy import or_

from dbsync.lang import *
from dbsync.utils import get_pk, class_mapper
from dbsync.core import synched_models
from dbsync.models import Operation, ContentType


def get_related_tables(sa_class):
    """Returns a list of related SA tables dependent on the given SA
    model by foreign key."""
    mapper = class_mapper(sa_class)
    models = synched_models.itervalues()
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


def related_content_types(operation, content_types):
    """For the given operation, return a list of content types that
    are dependent on it by foreign key."""
    parent_ct = lookup(attr("content_type_id") == operation.content_type_id,
                       content_types)
    if parent_ct is None:
        return []
    parent_model = synched_models.get(parent_ct.model_name, None)
    if parent_model is None:
        return []
    related_tables = get_related_tables(parent_model)
    return filter(bool,
                  [lookup(attr("table_name") == table.name, content_types)
                   for table in related_tables])


def related_row_ids(operation, content_types, session):
    """For the given operation, return a set of row id values that
    correspond to objects that are dependent by foreign key on the
    object being operated upon."""
    parent_ct = lookup(attr("content_type_id") == operation.content_type_id,
                       content_types)
    if parent_ct is None:
        return set()
    parent_model = synched_models.get(parent_ct.model_name, None)
    if parent_model is None:
        return set()
    parent_pk = get_pk(parent_model)
    related_tables = get_related_tables(parent_model)

    def get_model(table):
        return synched_models.get(
            maybe(lookup(attr("table_name") == table.name, content_types),
                  attr("model_name")),
            None)

    mapped_fks = ifilter(lambda (m, fks): m is not None and fks,
                         [(get_model(t),
                           get_fks(t, class_mapper(parent_model).mapped_table))
                          for t in related_tables])
    return set(getattr(obj, get_pk(obj))
               for model, fks in mapped_fks
               for obj in session.query(model).\
                   filter(or_(*(getattr(model, fk) == operation.row_id
                                for fk in fks))))


def find_direct_conflicts(unversioned_ops, pull_ops):
    """Detect conflicts where there's both unversioned and pulled
    operations, update or delete ones, referering to the same tracked
    object. This procedure relies on the uniqueness of the primary
    keys through time."""
    return [
        (pull_op, local_op)
        for pull_op in pull_ops
        if pull_op.command == 'u' or pull_op.command == 'd'
        for local_op in unversioned_ops
        if local_op.command == 'u' or local_op.command == 'd'
        if pull_op.row_id == local_op.row_id
        if pull_op.content_type_id == local_op.content_type_id]


def find_dependency_conflicts(unversioned_ops, pull_ops, content_types, session):
    """Detect conflicts by relationship dependency: deletes on the
    pull message on objects that have dependent objects inserted or
    updated on the local database."""
    return [
        (pull_op, local_op)
        for pull_op in pull_ops
        if pull_op.command == 'd'
        for local_op in unversioned_ops
        if local_op.command == 'i' or local_op.command == 'u'
        if local_op.content_type in related_content_types(pull_op, content_types)
        if local_op.row_id in related_row_ids(pull_op, content_types, session)]
