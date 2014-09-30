"""
.. module:: client.conflicts
   :synopsis: Conflict detection for the local merge operation.

This module handles the conflict detection that's required for the
local merge operation. The resolution phase is embedded in the
dbsync.client.pull module.

Related reading:

Gerritsen, Jan-Henk. Detecting synchronization conflicts for
horizontally decentralized relational databases. `Link to pdf`__.

.. __: http://essay.utwente.nl/61767/1/Master_thesis_Jan-Henk_Gerritsen.pdf
"""

from sqlalchemy import or_
from sqlalchemy.orm import undefer
from sqlalchemy.schema import UniqueConstraint

from dbsync.lang import *
from dbsync.utils import get_pk, class_mapper, query_model, column_properties
from dbsync.core import synched_models
from dbsync.models import Operation, ContentType


def get_related_tables(sa_class):
    """
    Returns a list of related SA tables dependent on the given SA
    model by foreign key.
    """
    mapper = class_mapper(sa_class)
    models = synched_models.itervalues()
    return [table for table in (class_mapper(model).mapped_table
                                for model in models)
            if mapper.mapped_table in [key.column.table
                                       for key in table.foreign_keys]]


def get_fks(table_from, table_to):
    """
    Returns the names of the foreign keys that are defined in
    *table_from* SA table and that refer to *table_to* SA table. If
    the foreign keys don't exist, this procedure returns an empty
    list.
    """
    fks = filter(lambda k: k.column.table == table_to, table_from.foreign_keys)
    return [fk.parent.name for fk in fks]


def related_local_ids(operation, content_types, session):
    """
    For the given operation, return a set of row id values mapped to
    content type ids that correspond to objects that are dependent by
    foreign key on the object being operated upon. The lookups are
    performed in the local database.
    """
    parent_ct = lookup(attr('content_type_id') == operation.content_type_id,
                       content_types)
    if parent_ct is None:
        return set()
    parent_model = synched_models.get(parent_ct.model_name, None)
    if parent_model is None:
        return set()
    related_tables = get_related_tables(parent_model)

    def get_model(table):
        return synched_models.get(
            maybe(lookup(attr('table_name') == table.name, content_types),
                  attr('model_name')),
            None)

    def ct_for_model(model):
        return lookup(attr('model_name') == model.__name__,
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
    """
    Like *related_local_ids*, but the lookups are performed in
    *container*, that's an instance of
    *dbsync.messages.base.BaseMessage*.
    """
    parent_ct = lookup(attr('content_type_id') == operation.content_type_id,
                       content_types)
    if parent_ct is None:
        return set()
    parent_model = synched_models.get(parent_ct.model_name, None)
    if parent_model is None:
        return set()
    related_tables = get_related_tables(parent_model)

    def get_model(table):
        return synched_models.get(
            maybe(lookup(attr('table_name') == table.name, content_types),
                  attr('model_name')),
            None)

    def ct_for_model(model):
        return lookup(attr('model_name') == model.__name__,
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
    """
    Detect conflicts where there's both unversioned and pulled
    operations, update or delete ones, referering to the same tracked
    object. This procedure relies on the uniqueness of the primary
    keys through time.
    """
    return [
        (pull_op, local_op)
        for pull_op in pull_ops
        if pull_op.command == 'u' or pull_op.command == 'd'
        for local_op in unversioned_ops
        if local_op.command == 'u' or local_op.command == 'd'
        if pull_op.row_id == local_op.row_id
        if pull_op.content_type_id == local_op.content_type_id]


def find_dependency_conflicts(pull_ops, unversioned_ops,
                              content_types,
                              session):
    """
    Detect conflicts by relationship dependency: deletes on the pull
    message on objects that have dependent objects inserted or updated
    on the local database.
    """
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


def find_reversed_dependency_conflicts(pull_ops, unversioned_ops,
                                       content_types,
                                       pull_message):
    """
    Deletes on the local database on objects that are referenced by
    inserted or updated objects in the pull message.
    """
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
    """
    Inserts over the same object. These conflicts should be resolved
    by keeping both objects, but moving the local one out of the way
    (reinserting it to get a new primary key). It should be possible,
    however, to specify a custom handler for cases where the primary
    key is a meaningful property of the object.
    """
    return [
        (pull_op, local_op)
        for local_op in unversioned_ops
        if local_op.command == 'i'
        for pull_op in pull_ops
        if pull_op.command == 'i'
        if pull_op.row_id == local_op.row_id
        if pull_op.content_type_id == local_op.content_type_id]


def find_unique_conflicts(pull_ops, unversioned_ops,
                          content_types,
                          pull_message,
                          session):
    """
    Unique constraints violated in a model. Returns two lists of
    dictionaries, the first one with the solvable conflicts, and the
    second one with the proper errors. Each conflict is a dictionary
    with the following fields::

        object: the local conflicting object, bound to the session
        columns: tuple of column names in the unique constraint
        new_values: tuple of values that can be used to update the
                    conflicting object

    Each error is a dictionary with the following fields::

        model: the model (class) of the conflicting object
        pk: the value of the primary key of the conflicting object
        columns: tuple of column names in the unique constraint
    """

    def verify_constraint(model, columns, values):
        """
        Checks to see whether some local object exists with
        conflicting values.
        """
        match = query_model(session, model, only_pk=True).\
            options(*(undefer(column) for column in columns)).\
            filter_by(**dict((column, value)
                             for column, value in izip(columns, values))).first()
        pk = get_pk(model)
        return match, getattr(match, pk, None)

    def get_remote_values(model, row_id, columns):
        """
        Gets the conflicting values out of the remote object set
        (*container*).
        """
        obj = pull_message.query(model).filter(attr('__pk__') == row_id).first()
        if obj is not None:
            return tuple(getattr(obj, column) for column in columns)
        return (None,)

    # keyed to content type
    unversioned_pks = dict((ct_id, set(op.row_id for op in unversioned_ops
                                       if op.content_type_id == ct_id
                                       if op.command != 'd'))
                           for ct_id in set(operation.content_type_id
                                            for operation in unversioned_ops))
    # the lists to fill with conflicts and errors
    conflicts, errors = [], []

    for op in pull_ops:
        ct = lookup(attr('content_type_id') == op.content_type_id, content_types)
        model = synched_models.get(ct.model_name, None)

        for constraint in ifilter(lambda c: isinstance(c, UniqueConstraint),
                                  class_mapper(model).mapped_table.constraints):

            unique_columns = tuple(col.name for col in constraint.columns)
            # Unique values on the server, to check conflicts with local database
            remote_values = get_remote_values(model, op.row_id, unique_columns)

            obj_conflict, pk_conflict = verify_constraint(
                model, unique_columns, remote_values)

            is_unversioned = pk_conflict in unversioned_pks.get(
                ct.content_type_id, set())

            if all(value is None for value in remote_values): continue # Null value
            if pk_conflict is None: continue # No problem
            if pk_conflict == op.row_id:
                if op.command == 'i':
                    # Two nodes created objects with the same unique
                    # value and same pk
                    errors.append(
                        {'model': type(obj_conflict),
                         'pk': pk_conflict,
                         'columns': unique_columns})
                continue

            # if pk_conflict != op.row_id:
            remote_obj = pull_message.query(model).\
                filter(attr('__pk__') == pk_conflict).first()

            if remote_obj is not None and not is_unversioned:
                old_values = tuple(getattr(obj_conflict, column)
                                   for column in unique_columns)
                # The new unique value of the conflictive object
                # in server
                new_values = tuple(getattr(remote_obj, column)
                                   for column in unique_columns)

                if old_values != new_values:
                    # Library error
                    # It's necesary to first update the unique value
                    session.refresh(obj_conflict, column_properties(obj_conflict))
                    conflicts.append(
                        {'object': obj_conflict,
                         'columns': unique_columns,
                         'new_values': new_values})
                else:
                    # The server allows two identical unique values
                    # This should be impossible
                    pass
            elif remote_obj is not None and is_unversioned:
                # Two nodes created objects with the same unique
                # values. Human error.
                errors.append(
                    {'model': type(obj_conflict),
                     'pk': pk_conflict,
                     'columns': unique_columns})
            else:
                # The conflicting object hasn't been modified on the
                # server, which must mean the local user is attempting
                # an update that collides with one from another user.
                errors.append(
                    {'model': type(obj_conflict),
                     'pk': pk_conflict,
                     'columns': unique_columns})
    return conflicts, errors
