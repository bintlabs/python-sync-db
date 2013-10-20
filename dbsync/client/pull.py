"""
Pull, merge and related operations.
"""

import re

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from dbsync.lang import *
from dbsync.utils import class_mapper, get_pk, query_model
from dbsync import core
from dbsync.models import ContentType, Operation
from dbsync.messages.pull import PullMessage
from dbsync.client.compression import compress, compressed_operations
from dbsync.client.conflicts import (
    get_related_tables,
    get_fks,
    find_direct_conflicts,
    find_dependency_conflicts,
    find_reversed_dependency_conflicts,
    find_insert_conflicts)
from dbsync.client.net import get_request


# Utilities specific to the merge

def max_local(ct, session):
    """Returns the maximum value for the primary key of the given
    content type in the local database."""
    model = core.synched_models.get(ct.model_name)
    if model is None:
        raise ValueError("can't find model for content type {0}".format(ct))
    return session.query(func.max(getattr(model, get_pk(model)))).scalar()

def max_remote(ct, container):
    """Returns the maximum value for the primary key of the given
    content type in the container."""
    return max(getattr(obj, get_pk(obj))
               for obj in container.query(ct.model_name))

def update_local_id(old_id, new_id, ct, content_types, session):
    """Updates the tuple matching *old_id* with *new_id*, and updates
    all dependent tuples in other tables as well."""
    # Updating either the tuple or the dependent tuples first would
    # cause integrity violations if the transaction is flushed in
    # between. The order doesn't matter.
    model = core.synched_models.get(ct.model_name)
    if model is None:
        raise ValueError("can't find model for content type {0}".format(ct))
    obj = query_model(session, model, only_pk=True).\
        filter_by(**{get_pk(model): old_id}).first()
    setattr(obj, get_pk(model), new_id)
    # Then the dependent ones
    def get_model(table):
        return core.synched_models.get(
            maybe(lookup(attr("table_name") == table.name, content_types),
                  attr("model_name")),
            None)
    related_tables = get_related_tables(model)
    mapped_fks = ifilter(lambda (m, fks): m is not None and fks,
                         [(get_model(t),
                           get_fks(t, class_mapper(model).mapped_table))
                          for t in related_tables])
    for model, fks in mapped_fks:
        for fk in fks:
            for obj in query_model(session, model).filter_by(**{fk: old_id}):
                setattr(obj, fk, new_id)
    session.flush() # raise integrity errors now


# TODO make enrich_error better at detecting columns and values
# This is frail but I can't think of another way
integrity_error_match = re.compile("^column\s(\w+)\sis\snot\sunique$").match

def enrich_error(error, operation, class_):
    """Fill the error with additional information, such as the class
    linked to the operation that triggered it, and the primary key of
    the object."""
    error.conflicting_class = class_
    error.conflicting_pk = operation.row_id
    error.conflicting_column = None
    if hasattr(error, 'orig'):
        orig = error.orig
        if hasattr(orig, 'message'):
            matches = integrity_error_match(orig.message)
            if matches:
                error.conflicting_column = matches.group(1)
    error.conflicting_value = None
    if error.conflicting_column is not None and \
            hasattr(error, 'statement') and \
            hasattr(error, 'params'):
        statement = error.statement
        col_tuple = "".join(takewhile(lambda c: c != ")",
                                      dropwhile(lambda c: c != "(", statement)))
        col_index = -1
        try:
            col_index = map(lambda s: s.strip(), col_tuple[1:].split(",")).\
                index(error.conflicting_column)
            error.conflicting_value = error.params[col_index]
        except ValueError: pass
    return error


@core.with_listening(False)
@core.with_transaction
def merge(pull_message, session=None):
    """Merges a message from the server with the local database.

    *pull_message* is an instance of dbsync.messages.pull.PullMessage."""
    if not isinstance(pull_message, PullMessage):
        raise TypeError("need an instance of dbsync.messages.pull.PullMessage "\
                            "to perform the local merge operation")
    content_types = session.query(ContentType).all()
    # preamble: detect conflicts between pulled operations and unversioned ones
    compress()
    unversioned_ops = session.query(Operation).\
        filter(Operation.version_id == None).\
        order_by(Operation.order.asc()).all()
    pull_ops = compressed_operations(pull_message.operations)

    direct_conflicts = find_direct_conflicts(pull_ops, unversioned_ops)

    # in which the delete operation is registered on the pull message
    dependency_conflicts = find_dependency_conflicts(
        pull_ops, unversioned_ops, content_types, session)

    # in which the delete operation was performed locally
    reversed_dependency_conflicts = find_reversed_dependency_conflicts(
        pull_ops, unversioned_ops, content_types, pull_message)

    insert_conflicts = find_insert_conflicts(pull_ops, unversioned_ops)

    # merge transaction
    # first phase: perform pull operations, when allowed and while
    # resolving conflicts
    def extract(op, conflicts):
        return [local for remote, local in conflicts if remote is op]

    def purgelocal(local):
        session.delete(local)
        exclude = lambda _, lop: lop is not local
        mfilter(exclude, direct_conflicts)
        mfilter(exclude, dependency_conflicts)
        mfilter(exclude, reversed_dependency_conflicts)
        mfilter(exclude, insert_conflicts)
        unversioned_ops.remove(local)

    for pull_op in pull_ops:
        # flag to control whether the remote operation is free of obstacles
        can_perform = True
        # the content type and class of the operation
        ct = lookup(attr('content_type_id') == pull_op.content_type_id,
                    content_types)
        class_ = core.synched_models.get(ct.model_name, None)

        direct = extract(pull_op, direct_conflicts)
        if direct:
            if pull_op.command == 'd':
                can_perform = False
            for local in direct:
                pair = (pull_op.command, local.command)
                if pair == ('u', 'u'):
                    can_perform = False # favor local changes over remote ones
                elif pair == ('u', 'd'):
                    pull_op.command = 'i' # negate the local delete
                    purgelocal(local)
                elif pair == ('d', 'u'):
                    local.command = 'i' # negate the remote delete 
                else: # ('d', 'd')
                    pass # nothing to do

        dependency = extract(pull_op, dependency_conflicts)
        if dependency:
            can_perform = False
            order = min(op.order for op in unversioned_ops)
            # first move all operations further in order, to make way
            # for the new one
            for op in unversioned_ops:
                op.order = op.order + 1
            session.flush()
            # then create operation to reflect the reinsertion and
            # maintain a correct operation history
            session.add(Operation(row_id=pull_op.row_id,
                                  content_type_id=pull_op.content_type_id,
                                  command='i',
                                  order=order))

        reversed_dependency = extract(pull_op, reversed_dependency_conflicts)
        for local in reversed_dependency:
            # reinsert record
            local.command = 'i'
            local.perform(content_types,
                          core.synched_models,
                          pull_message,
                          session)
            # delete trace of deletion
            purgelocal(local)

        insert = extract(pull_op, insert_conflicts)
        for local in insert:
            session.flush()
            next_id = max(max_remote(ct, pull_message),
                          max_local(ct, session)) + 1
            update_local_id(local.row_id, next_id, ct, content_types, session)
            local.row_id = next_id

        if can_perform:
            pull_op.perform(content_types,
                            core.synched_models,
                            pull_message,
                            session)
            try: session.flush()
            except IntegrityError as e: raise enrich_error(e, pull_op, class_)

    # second phase: insert versions from the pull_message
    for pull_version in pull_message.versions:
        session.add(pull_version)


class BadResponseError(Exception): pass


def pull(pull_url, extra_data=None):
    """Attempts a pull from the server. Returns the response body.

    Additional data can be passed to the request by giving
    *extra_data*, a dictionary of values.

    If not interrupted, the pull will perform a local merge. If the
    response from the server isn't appropriate, it will raise a
    dbysnc.client.pull.BadResponseError."""
    assert isinstance(pull_url, basestring), "pull url must be a string"
    assert bool(pull_url), "pull url can't be empty"
    if extra_data is not None:
        assert isinstance(extra_data, dict), "extra data must be a dictionary"
    extra = dict((k, v) for k, v in extra_data.iteritems()
                 if k != 'latest_version_id') \
                 if extra_data is not None else {}
    data = {'latest_version_id': core.get_latest_version_id()}
    data.update(extra)

    code, reason, response = get_request(pull_url, data)

    if (code // 100 != 2) or response is None:
        raise BadResponseError(code, reason, response)
    message = None
    try:
        message = PullMessage(response)
    except KeyError:
        raise BadResponseError(
            "response object isn't a valid PullMessage", response)
    merge(message)
    # return the response for the programmer to do what she wants
    # afterwards
    return response
