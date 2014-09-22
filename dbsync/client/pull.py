"""
Pull, merge and related operations.
"""

from sqlalchemy import func
from sqlalchemy.orm import make_transient

from dbsync.lang import *
from dbsync.utils import class_mapper, get_pk, query_model
from dbsync import core
from dbsync.models import ContentType, Operation
from dbsync.messages.pull import PullMessage, PullRequestMessage
from dbsync.client.compression import compress, compressed_operations
from dbsync.client.conflicts import (
    get_related_tables,
    get_fks,
    find_direct_conflicts,
    find_dependency_conflicts,
    find_reversed_dependency_conflicts,
    find_insert_conflicts,
    find_unique_conflicts)
from dbsync.client.net import post_request


# Utilities specific to the merge

def max_local(ct, session):
    """
    Returns the maximum value for the primary key of the given content
    type in the local database.
    """
    model = core.synched_models.get(ct.model_name)
    if model is None:
        raise ValueError("can't find model for content type {0}".format(ct))
    return session.query(func.max(getattr(model, get_pk(model)))).scalar()


def max_remote(ct, container):
    """
    Returns the maximum value for the primary key of the given content
    type in the container.
    """
    return max(getattr(obj, get_pk(obj))
               for obj in container.query(ct.model_name))


def update_local_id(old_id, new_id, ct, content_types, session):
    """
    Updates the tuple matching *old_id* with *new_id*, and updates all
    dependent tuples in other tables as well.
    """
    # Updating either the tuple or the dependent tuples first would
    # cause integrity violations if the transaction is flushed in
    # between. The order doesn't matter.
    model = core.synched_models.get(ct.model_name)
    if model is None:
        raise ValueError("can't find model for content type {0}".format(ct))
    # must load fully, don't know yet why
    obj = query_model(session, model).\
        filter_by(**{get_pk(model): old_id}).first()
    setattr(obj, get_pk(model), new_id)

    # Then the dependent ones
    def get_model(table):
        return core.synched_models.get(
            maybe(lookup(attr('table_name') == table.name, content_types),
                  attr('model_name')),
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
    session.flush()  # raise integrity errors now


class UniqueConstraintError(Exception):

    entries = None

    def __init__(self, entries):
        super(UniqueConstraintError, self).__init__(entries)
        self.entries = entries

    def __repr__(self):
        if not self.entries: return u"<UniqueConstraintError - empty>"
        return u"<UniqueConstraintError - {0}>".format(
            u"; ".join(
                u"{0} pk {1} columns ({2})".format(
                    entry['model'].__name__,
                    entry['pk'],
                    u", ".join(entry['columns']))
                for entry in self.entries))

    def __str__(self): return repr(self)


@core.with_listening(False)
@core.with_transaction()
def merge(pull_message, session=None):
    """
    Merges a message from the server with the local database.

    *pull_message* is an instance of dbsync.messages.pull.PullMessage.
    """
    if not isinstance(pull_message, PullMessage):
        raise TypeError("need an instance of dbsync.messages.pull.PullMessage "
                        "to perform the local merge operation")
    content_types = session.query(ContentType).all()
    valid_cts = set(ct.content_type_id for ct in content_types)

    compress()
    unversioned_ops = session.query(Operation).\
        filter(Operation.version_id == None).\
        order_by(Operation.order.asc()).all()
    pull_ops = filter(attr('content_type_id').in_(valid_cts),
                      pull_message.operations)
    pull_ops = compressed_operations(pull_ops)

    # I) first phase: resolve unique constraint conflicts if
    # possible. Abort early if a human error is detected
    unique_conflicts, unique_errors = find_unique_conflicts(
        pull_ops, unversioned_ops, content_types, pull_message, session)

    if unique_errors:
        raise UniqueConstraintError(unique_errors)

    conflicting_objects = set()
    for uc in unique_conflicts:
        obj = uc['object']
        conflicting_objects.add(obj)
        for key, value in izip(uc['columns'], uc['new_values']):
            setattr(obj, key, value)
    # Resolve potential cyclical conflicts by deleting and reinserting
    for obj in conflicting_objects:
        make_transient(obj) # remove from session
    for model in set(type(obj) for obj in conflicting_objects):
        pk_name = get_pk(model)
        pks = [getattr(obj, pk_name)
               for obj in conflicting_objects
               if type(obj) is model]
        session.query(model).filter(getattr(model, pk_name).in_(pks)).\
            delete(synchronize_session=False) # remove from the database
    session.add_all(conflicting_objects) # reinsert them
    session.flush()

    # II) second phase: detect conflicts between pulled operations and
    # unversioned ones
    direct_conflicts = find_direct_conflicts(pull_ops, unversioned_ops)

    # in which the delete operation is registered on the pull message
    dependency_conflicts = find_dependency_conflicts(
        pull_ops, unversioned_ops, content_types, session)

    # in which the delete operation was performed locally
    reversed_dependency_conflicts = find_reversed_dependency_conflicts(
        pull_ops, unversioned_ops, content_types, pull_message)

    insert_conflicts = find_insert_conflicts(pull_ops, unversioned_ops)

    # III) third phase: perform pull operations, when allowed and
    # while resolving conflicts
    def extract(op, conflicts):
        return [local for remote, local in conflicts if remote is op]

    def purgelocal(local):
        session.delete(local)
        exclude = lambda tup: tup[1] is not local
        mfilter(exclude, direct_conflicts)
        mfilter(exclude, dependency_conflicts)
        mfilter(exclude, reversed_dependency_conflicts)
        mfilter(exclude, insert_conflicts)
        unversioned_ops.remove(local)

    for pull_op in pull_ops:
        # flag to control whether the remote operation is free of obstacles
        can_perform = True
        # flag to detect the early exclusion of a remote operation
        reverted = False
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
                    session.flush()
                    reverted = True
                else: # ('d', 'd')
                    purgelocal(local)

        dependency = extract(pull_op, dependency_conflicts)
        if dependency and not reverted:
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

            session.flush()

    # IV) fourth phase: insert versions from the pull_message
    for pull_version in pull_message.versions:
        session.add(pull_version)


class BadResponseError(Exception):
    pass


def pull(pull_url, extra_data=None,
         encode=None, decode=None, headers=None, monitor=None, timeout=None,
         include_extensions=True,
         merge_mutex=None):
    """
    Attempts a pull from the server. Returns the response body.

    Additional data can be passed to the request by giving
    *extra_data*, a dictionary of values.

    If not interrupted, the pull will perform a local merge. If the
    response from the server isn't appropriate, it will raise a
    dbysnc.client.pull.BadResponseError.

    By default, the *encode* function is ``json.dumps``, the *decode*
    function is ``json.loads``, and the *headers* are appropriate HTTP
    headers for JSON.

    *monitor* should be a routine that receives a dictionary with
    information of the state of the request and merge procedure.

    *include_extensions* dictates whether the extension functions will
    be called during the merge or not. Default is ``True``.

    *merge_mutex*, if provided, is an object that complies with
    python's _with_ protocol and that will be used to wrap the merge
    call.
    """
    assert isinstance(pull_url, basestring), "pull url must be a string"
    assert bool(pull_url), "pull url can't be empty"
    if extra_data is not None:
        assert isinstance(extra_data, dict), "extra data must be a dictionary"
    compress()
    request_message = PullRequestMessage()
    request_message.add_unversioned_operations()
    data = request_message.to_json()
    data.update({'extra_data': extra_data or {}})

    code, reason, response = post_request(
        pull_url, data, encode, decode, headers, timeout, monitor)
    if (code // 100 != 2):
        if monitor:
            monitor({'status': "error", 'reason': reason.lower()})
        raise BadResponseError(code, reason, response)
    if response is None:
        if monitor:
            monitor({
                'status': "error",
                'reason': "invalid response format"})
        raise BadResponseError(code, reason, response)
    message = None
    try:
        message = PullMessage(response)
    except KeyError:
        if monitor:
            monitor({
                'status': "error",
                'reason': "invalid message format"})
        raise BadResponseError(
            "response object isn't a valid PullMessage", response)

    if monitor:
        monitor({
            'status': "merging",
            'operations': len(message.operations)})
    if merge_mutex is not None:
        with merge_mutex:
            merge(message, include_extensions=include_extensions)
    else:
        merge(message, include_extensions=include_extensions)
    if monitor:
        monitor({'status': "done"})
    # return the response for the programmer to do what she wants
    # afterwards
    return response
