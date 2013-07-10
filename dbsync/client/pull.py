"""
Pull, merge and related operations.
"""

from sqlalchemy import or_

from dbsync.lang import *
from dbsync.utils import get_pk, get_related_tables, get_fks, class_mapper
from dbsync.core import Session, synched_models
from dbsync.models import Operation, ContentType
from dbsync.messages.pull import compressed_operations
from dbsync.client.push import compress


def related_content_types(operation, content_types=None):
    """For the given operation, return a list of content types that
    are dependent on it by foreign key."""
    if content_types is None:
        session = Session()
        content_types = session.query(ContentType).all()
    parent_ct = lookup(attr("content_type_id") == operation.content_type_id,
                       content_types)
    if parent_ct is None:
        return []
    parent_model = synched_models.get(parent_ct.model_name, None)
    if parent_model is None:
        return []
    related_tables = get_related_tables(parent_model,
                                        synched_models.itervalues())
    return filter(bool,
                  [lookup(attr("table_name") == table.name, content_types)
                   for table in related_tables])


def related_row_ids(operation, content_types=None):
    """For the given operation, return a set of row id values that
    correspond to objects that are dependent by foreign key on the
    object being operated upon."""
    session = Session()
    if content_types is None:
        content_types = session.query(ContentType).all()
    parent_ct = lookup(attr("content_type_id") == operation.content_type_id,
                       content_types)
    if parent_ct is None:
        return set()
    parent_model = synched_models.get(parent_ct.model_name, None)
    if parent_model is None:
        return set()
    parent_pk = get_pk(parent_model)
    related_tables = get_related_tables(parent_model,
                                        synched_models.itervalues())

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


def merge(pull_message):
    """Merges a message from the server with the local database.

    *pull_message* is an instance of dbsync.messages.pull.PullMessage."""
    # preamble: detect conflicts between pulled operations and unversioned ones
    session = Session()
    compress() # avoid redundancy in conflict detection
    content_types = session.query(ContentType).all()
    unversioned_ops = session.query(Operation).\
        filter(Operation.version_id == None).order_by(Operation.order.asc())
    pull_ops = compressed_operations(pull_message.operations)
    conflicts = [
        (pull_op, local_op)
        for pull_op in pull_ops
        if pull_op.command == 'u' or pull_op.command == 'd'
        for local_op in unversioned_ops
        if local_op.command == 'u' or local_op.command == 'd'
        if pull_op.row_id == local_op.row_id
        if pull_op.content_type_id == local_op.content_type_id]
    # detect conflicts by relationship dependency: deletes on the pull
    # message on objects that have dependent objects inserted or
    # updated on the local database.
    dependency_conflicts = [
        (pull_op, local_op)
        for pull_op in pull_ops
        if pull_op.command == 'd'
        for local_op in unversioned_ops
        if local_op.command == 'i' or local_op.command == 'u'
        if local_op.content_type in related_content_types(pull_op, content_types)
        if local_op.row_id in related_row_ids(pull_op, content_types)]
    # merge transaction
    # first phase: move the local operations and objects out of the way
    # TODO first phase
