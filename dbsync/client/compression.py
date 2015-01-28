"""
Operation compression, both in-memory and in-database.
"""

import warnings

from dbsync.lang import *
from dbsync.utils import get_pk, query_model
from dbsync import core
from dbsync.models import Version, Operation
from dbsync.logs import get_logger


logger = get_logger(__name__)


def _assert_operation_sequence(seq, session=None):
    """
    Asserts the correctness of a sequence of operations over a single
    tracked object.

    The sequence is given in a sorted state, from newest operation to
    oldest.
    """
    message = "The sequence of operations for the given object "\
        "<row_id: {0}, content_type_id: {1}, model: {2}> is inconsistent. "\
        "This might indicate external interference with the synchronization "\
        "model or, most commonly, the reuse of old primary keys by the "\
        "database engine. To function properly, the database engine must use "\
        "unique primary keys through the history of the table "\
        "(e.g. using AUTO INCREMENT). Operations from old to new: {3}".\
        format(seq[0].row_id,
               seq[0].content_type_id,
               seq[0].tracked_model,
               list(reversed(map(attr('command'), seq))))

    if not all(op.command == 'u' for op in seq[1:-1]):
        warnings.warn(message)
        logger.error(
            u"Can't have anything but updates between beginning "
            u"and end of operation sequence. %s",
            seq)

    if len(seq) > 1:
        if seq[-1].command == 'd':
            warnings.warn(message)
            logger.error(
                u"Can't have anything after a delete operation in sequence. %s",
                seq)

        if seq[0].command == 'i':
            warnings.warn(message)
            logger.error(
                u"Can't have anything before an insert in operation sequence. %s",
                seq)


def compress(session=None):
    """
    Compresses unversioned operations in the database.

    For each row in the operations table, this deletes unnecesary
    operations that would otherwise bloat the message.

    This procedure is called internally before the 'push' request
    happens, and before the local 'merge' happens.
    """
    closeit = session is None
    session = session if not closeit else core.Session()
    unversioned = session.query(Operation).\
        filter(Operation.version_id == None).order_by(Operation.order.desc())
    seqs = group_by(lambda op: (op.row_id, op.content_type_id), unversioned)

    # Check errors on sequences
    for seq in seqs.itervalues():
        _assert_operation_sequence(seq, session)

    for seq in ifilter(lambda seq: len(seq) > 1, seqs.itervalues()):
        if seq[-1].command == 'i':
            if all(op.command == 'u' for op in seq[:-1]):
                # updates are superfluous
                map(session.delete, seq[:-1])
            elif seq[0].command == 'd':
                # it's as if the object never existed
                map(session.delete, seq)
        elif seq[-1].command == 'u':
            if all(op.command == 'u' for op in seq[:-1]):
                # leave a single update
                map(session.delete, seq[1:])
            elif seq[0].command == 'd':
                # leave the delete statement
                map(session.delete, seq[1:])
    session.flush()

    # repair inconsistencies
    for operation in session.query(Operation).\
            filter(Operation.version_id == None).\
            order_by(Operation.order.desc()).all():
        session.flush()
        model = operation.tracked_model
        if not model:
            logger.error(
                "operation linked to content type "
                "not tracked: %s" % operation.content_type_id)
            continue
        if operation.command in ('i', 'u'):
            if query_model(session, model, only_pk=True).\
                    filter_by(**{get_pk(model): operation.row_id}).count() == 0:
                logger.warning(
                    "deleting operation %s for model %s "
                    "for absence of backing object" % (operation, model.__name__))
                session.delete(operation)
                continue
        if operation.command == 'u':
            subsequent = session.query(Operation).\
                filter(Operation.content_type_id == operation.content_type_id,
                       Operation.version_id == None,
                       Operation.row_id == operation.row_id,
                       Operation.order > operation.order).all()
            if any(op.command == 'i' for op in subsequent) and \
                    all(op.command != 'd' for op in subsequent):
                logger.warning(
                    "deleting update operation %s for model %s "
                    "for preceding an insert operation" %\
                        (operation, model.__name__))
                session.delete(operation)
                continue
        if session.query(Operation).\
                filter(Operation.content_type_id == operation.content_type_id,
                       Operation.command == operation.command,
                       Operation.version_id == None,
                       Operation.row_id == operation.row_id,
                       Operation.order != operation.order).count() > 0:
            logger.warning(
                "deleting operation %s for model %s "
                "for being redundant after compression" %\
                    (operation, model.__name__))
            session.delete(operation)
            continue
    result = session.query(Operation).\
        filter(Operation.version_id == None).\
        order_by(Operation.order.asc()).all()
    if closeit:
        session.commit()
        session.close()
    else:
        session.flush()
    return result


def compressed_operations(operations):
    """
    Compresses a set of operations so as to avoid redundant
    ones. Returns the compressed set sorted by operation order. This
    procedure doesn't perform database operations.
    """
    seqs = group_by(lambda op: (op.row_id, op.content_type_id),
                    sorted(operations, key=attr('order')))
    compressed = []
    for seq in seqs.itervalues():
        if len(seq) == 1:
            compressed.append(seq[0])
        elif seq[0].command == 'i':
            if seq[-1].command == 'd':
                pass
            else:
                compressed.append(seq[0])
        elif seq[0].command == 'u':
            if seq[-1].command == 'd':
                compressed.append(seq[-1])
            else:
                compressed.append(seq[0])
        else: # seq[0].command == 'd':
            if seq[-1].command == 'd':
                compressed.append(seq[0])
            elif seq[-1].command == 'u':
                compressed.append(seq[-1])
            else: # seq[-1].command == 'i':
                op = seq[-1]
                compressed.append(
                    Operation(order=op.order,
                              content_type_id=op.content_type_id,
                              row_id=op.row_id,
                              version_id=op.version_id,
                              command='u'))
    compressed.sort(key=attr('order'))
    return compressed


def unsynched_objects():
    """
    Returns a list of triads (class, id, operation) that represents
    the unsynchronized objects in the tracked database.

    The first element of each triad is the class for the
    unsynchronized object.

    The second element is the primary key *value* of the object.

    The third element is a character in ``('i', 'u', 'd')`` that
    represents the operation that altered the objects state (insert,
    update or delete). If it's a delete, the object won't be present
    in the tracked database.

    Because of compatibility issues, this procedure will only return
    triads for classes marked for both push and pull handling.
    """
    session = core.Session()
    ops = compress(session)
    def getclass(op):
        class_ = op.tracked_model
        if class_ is None: return None
        if class_ not in core.pulled_models or class_ not in core.pushed_models:
            return None
        return class_
    triads = [
        (c, op.row_id, op.command)
        for c, op in ((getclass(op), op) for op in ops)
        if c is not None]
    session.commit()
    session.close()
    return triads


@core.with_transaction()
def trim(session=None):
    "Trims the internal synchronization tables, to free space."
    last_id = core.get_latest_version_id(session=session)
    session.query(Operation).filter(Operation.version_id != None).delete()
    session.query(Version).filter(Version.version_id != last_id).delete()
