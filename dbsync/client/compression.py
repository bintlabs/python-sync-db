"""
Operation compression, both in-memory and in-database.
"""

import warnings

from dbsync.lang import *
from dbsync import core
from dbsync.models import Version, Operation, ContentType
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
        "<row_id: {0}, content_type_id: {1}> is inconsistent. "\
        "This might indicate external interference with the synchronization "\
        "model or, most commonly, the reuse of old primary keys by the "\
        "database engine. To function properly, the database engine must use "\
        "unique primary keys through the history of the table "\
        "(e.g. using AUTO INCREMENT). Operations from old to new: {2}".\
        format(seq[0].row_id,
               seq[0].content_type_id,
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
            # repair the sequence
            # if session is not None:
                # map(session.delete, seq[:-1])

        if seq[0].command == 'i':
            warnings.warn(message)
            logger.error(
                u"Can't have anything before an insert in operation sequence. %s",
                seq)
            # repair the sequence
            if session is not None:
                map(session.delete, seq[1:])


def compress():
    """
    Compresses unversioned operations in the database.

    For each row in the operations table, this deletes unnecesary
    operations that would otherwise bloat the message.

    This procedure is called internally before the 'push' request
    happens, and before the local 'merge' happens.
    """
    session = core.Session()
    unversioned = session.query(Operation).\
        filter(Operation.version_id == None).order_by(Operation.order.desc())
    seqs = group_by(lambda op: (op.row_id, op.content_type_id), unversioned)

    # Check errors on sequences, and repair if needed
    for seq in seqs.itervalues():
        _assert_operation_sequence(seq, session)

    for seq in ifilter(lambda seq: len(seq) > 1, seqs.itervalues()):
        if seq[-1].command == 'i':
            if andmap(attr('command') == 'u', seq[:-1]):
                # updates are superfluous
                map(session.delete, seq[:-1])
            elif seq[0].command == 'd':
                # it's as if the object never existed
                map(session.delete, seq)
        elif seq[-1].command == 'u':
            if andmap(attr('command') == 'u', seq[:-1]):
                # leave a single update
                map(session.delete, seq[1:])
            elif seq[0].command == 'd':
                # leave the delete statement
                map(session.delete, seq[1:])
    session.commit()
    session.close()


def compressed_operations(operations):
    """
    Compresses as set of operations so as to avoid redundant
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
    """
    compress()
    session = core.Session()
    cts = session.query(ContentType).all()
    ops = session.query(Operation).filter(Operation.version_id == None).all()
    def getclass(ct_id):
        return core.synched_models.get(
            maybe(lookup(attr('content_type_id') == ct_id, cts),
                  attr('model_name'),
                  None),
            None)
    triads = [
        (c, op.row_id, op.command)
        for c, op in ((getclass(op.content_type_id), op) for op in ops)
        if c is not None]
    session.close()
    return triads


@core.with_transaction()
def trim(session=None):
    "Trims the internal synchronization tables, to free space."
    last_id = core.get_latest_version_id(session=session)
    session.query(Operation).filter(Operation.version_id != None).delete()
    session.query(Version).filter(Version.version_id != last_id).delete()
