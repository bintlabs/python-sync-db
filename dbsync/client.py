"""
Interface for the synchronization client.

The client or node emits 'push' and 'pull' requests to the server. The
client can also request a registry key if it hasn't been given one
yet.
"""

import logging
from itertools import ifilter

from sqlalchemy import event

from dbsync.lang import *
from dbsync import core
from dbsync.models import Operation, ContentType


#: networking configuration
_networking_parameters = {
    "server_url": "localhost",
    "encrypt": False, # specific algorithm
    }


def configure_networking(**kwargs):
    """Configurate the networking aspect of the client library.

    Documentation pending"""
    for k in kwargs:
        if k not in _networking_paremeters:
            raise ValueError("unknown configuration parameter: {0}".format(k))
        _networking_parameters = kwargs[k]
    return _networking_parameters


def make_listener(command):
    """Builds a listener for the given command (i, u, d)."""
    def listener(mapper, connection, target):
        if not core.listening: return
        session = core.Session()
        tname = mapper.mapped_table.name
        ct = session.query(ContentType).\
            filter(ContentType.table_name == tname).first()
        if ct is None:
            logging.error("you must register a content type for {0}"\
                              "to keep track of operations".format(tname))
            return
        pk = getattr(target, mapper.primary_key[0].name)
        op = Operation(
            row_id=pk,
            version_id=None, # operation not yet versioned
            content_type_id=ct.content_type_id,
            command=command)
        session.add(op)
        session.commit()
    return listener


def track(model):
    """Adds an ORM class to the list of synchronized classes.

    It can be used as a class decorator. This will also install
    listeners to keep track of CUD operations for the given model."""
    core.synched_models.append(model)
    event.listen(model, 'after_insert', make_listener('i'))
    event.listen(model, 'after_update', make_listener('u'))
    event.listen(model, 'after_delete', make_listener('d'))
    return model


def compress():
    """Compresses unversioned operations.

    For each row in the operations table, this deletes unnecesary
    operations that would otherwise bloat the message.

    This procedure is called internally before the 'push' request
    happens."""
    session = core.Session()
    unversioned = session.query(Operation).\
        filter(Operation.version_id == None).order_by(Operation.order.desc())
    sequences = group_by(lambda op: (op.row_id, op.content_type_id), unversioned)

    for _, seq in ifilter(lambda _, seq: len(seq) > 1, sequences.iteritems()):
        if seq[-1].command == 'i':
            if andmap(attr("command") == 'u', seq[:-1]):
                # updates are superfluous
                map(session.delete, seq[:-1])
            elif seq[0].command == 'd':
                # it's as if the object never existed
                map(session.delete, seq)
        elif seq[-1].command == 'u':
            if andmap(attr("command") == 'u', seq[:-1]):
                # leave a single update
                map(session.delete, seq[1:])
            elif seq[0].command == 'd':
                # leave the delete statement
                map(session.delete, seq[1:])
    session.commit()


def merge(pull_message, node_session):
    """Merges a message from the server with the local database.

    * *pull_message* is an instace of
     dbsync.messages.pull.PullMessage.

    * *node_session* is a valid sqlalchemy session used to access the
     node's database."""
    # TODO merge and conflict resolution
    pass
