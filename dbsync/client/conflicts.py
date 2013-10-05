"""
Conflict detection for the local merge operation.

This module handles the conflict detection that's required for the
local merge operation. The resolution phase is embedded in the
dbsync.client.pull module.

Related reading:

Gerritsen, Jan-Henk. Detecting synchronization conflicts for
horizontally decentralized relational databases. `Link to pdf`__.

.. __: http://essay.utwente.nl/61767/1/Master_thesis_Jan-Henk_Gerritsen.pdf
"""

from sqlalchemy import or_

from dbsync.lang import *
from dbsync.utils import get_pk, class_mapper, query_model
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


def related_local_ids(operation, content_types, session):
    """For the given operation, return a set of row id values mapped
    to content type ids that correspond to objects that are dependent
    by foreign key on the object being operated upon. The lookups are
    performed in the local database."""
    parent_ct = lookup(attr("content_type_id") == operation.content_type_id,
                       content_types)
    if parent_ct is None:
        return set()
    parent_model = synched_models.get(parent_ct.model_name, None)
    if parent_model is None:
        return set()
    related_tables = get_related_tables(parent_model)

    def get_model(table):
        return synched_models.get(
            maybe(lookup(attr("table_name") == table.name, content_types),
                  attr("model_name")),
            None)

    def ct_for_model(model):
        return lookup(attr("model_name") == model.__name__,
                      content_types)

    mapped_fks = ifilter(lambda (m, fks): m is not None and fks,
                         [(get_model(t),
                           get_fks(t, class_mapper(parent_model).mapped_table))
                          for t in related_tables])
    return set(
        (pk, ct.content_type_id)
        for pk, ct in \
            ((getattr(obj, get_pk(obj)), ct_for_model(model))
             for model, fks in mapped_fks
             for obj in query_model(session, model, only_pk=True).\
                 filter(or_(*(getattr(model, fk) == operation.row_id
                              for fk in fks))).all())
        if ct is not None)


def related_remote_ids(operation, content_types, container):
    """Like *related_local_ids*, but the lookups are performed in
    *container*, that's an instance of
    *dbsync.messages.base.BaseMessage*."""
    parent_ct = lookup(attr("content_type_id") == operation.content_type_id,
                       content_types)
    if parent_ct is None:
        return set()
    parent_model = synched_models.get(parent_ct.model_name, None)
    if parent_model is None:
        return set()
    related_tables = get_related_tables(parent_model)

    def get_model(table):
        return synched_models.get(
            maybe(lookup(attr("table_name") == table.name, content_types),
                  attr("model_name")),
            None)

    def ct_for_model(model):
        return lookup(attr("model_name") == model.__name__,
                      content_types)

    mapped_fks = ifilter(lambda (m, fks): m is not None and fks,
                         [(get_model(t),
                           get_fks(t, class_mapper(parent_model).mapped_table))
                          for t in related_tables])
    return set(
        (pk, ct.content_type_id)
        for pk, ct in \
            ((getattr(obj, get_pk(obj)), ct_for_model(model))
             for model, fks in mapped_fks
             for obj in container.query(model).\
                 filter(lambda obj: any(getattr(obj, fk) == operation.row_id
                                        for fk in fks)))
        if ct is not None)


def find_direct_conflicts(pull_ops, unversioned_ops):
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


def find_dependency_conflicts(pull_ops, unversioned_ops, content_types, session):
    """Detect conflicts by relationship dependency: deletes on the
    pull message on objects that have dependent objects inserted or
    updated on the local database."""
    related_ids = dict(
        (pull_op, related_local_ids(pull_op, content_types, session))
        for pull_op in pull_ops
        if pull_op.command == 'd')
    return [
        (pull_op, local_op)
        for pull_op in pull_ops
        if pull_op.command == 'd'
        for local_op in unversioned_ops
        if local_op.command == 'i' or local_op.command == 'u'
        if (local_op.row_id, local_op.content_type_id) in related_ids[pull_op]]


def find_reversed_dependency_conflicts(pull_ops,
                                       unversioned_ops,
                                       content_types,
                                       pull_message):
    """Deletes on the local database on objects that are referenced by
    inserted or updated objects in the pull message."""
    related_ids = dict(
        (local_op, related_remote_ids(local_op, content_types, pull_message))
        for local_op in unversioned_ops
        if local_op.command == 'd')
    return [
        (pull_op, local_op)
        for local_op in unversioned_ops
        if local_op.command == 'd'
        for pull_op in pull_ops
        if pull_op.command == 'i' or pull_op.command == 'u'
        if (pull_op.row_id, pull_op.content_type_id) in related_ids[local_op]]


def find_insert_conflicts(pull_ops, unversioned_ops):
    """Inserts over the same object. These conflicts should be
    resolved by keeping both objects, but moving the local one out of
    the way (reinserting it to get a new primary key). It should be
    possible, however, to specify a custom handler for cases where the
    primary key is a meaningful property of the object."""
    return [
        (pull_op, local_op)
        for local_op in unversioned_ops
        if local_op.command == 'i'
        for pull_op in pull_ops
        if pull_op.command == 'i'
        if pull_op.row_id == local_op.row_id
        if pull_op.content_type_id == local_op.content_type_id]
