"""
.. module:: server.conflicts
   :synopsis: Conflict detection for the centralized push operation.
"""

from sqlalchemy.schema import UniqueConstraint

from dbsync.lang import *
from dbsync.utils import get_pk, class_mapper, query_model, column_properties


def find_unique_conflicts(push_message, session):
    """
    Returns a list of conflicts caused by unique constraints in the
    given push message contrasted against the database. Each conflict
    is a dictionary with the following fields::

        object: the conflicting object in database, bound to the
                session
        columns: tuple of column names in the unique constraint
        new_values: tuple of values that can be used to update the
                    conflicting object.
    """
    conflicts = []

    for pk, model in ((op.row_id, op.tracked_model)
                      for op in push_message.operations
                      if op.command != 'd'):
        if model is None: continue

        for constraint in ifilter(lambda c: isinstance(c, UniqueConstraint),
                                  class_mapper(model).mapped_table.constraints):

            unique_columns = tuple(col.name for col in constraint.columns)
            remote_obj = push_message.query(model).\
                filter(attr('__pk__') == pk).first()
            remote_values = tuple(getattr(remote_obj, col, None)
                                  for col in unique_columns)

            if all(value is None for value in remote_values): continue
            local_obj = query_model(session, model).\
                filter_by(**dict(izip(unique_columns, remote_values))).first()
            if local_obj is None: continue
            local_pk = getattr(local_obj, get_pk(model))
            if local_pk == pk: continue

            push_obj = push_message.query(model).\
                filter(attr('__pk__') == local_pk).first()
            if push_obj is None: continue # push will fail

            conflicts.append(
                {'object': local_obj,
                 'columns': unique_columns,
                 'new_values': tuple(getattr(push_obj, col)
                                     for col in unique_columns)})

    return conflicts
