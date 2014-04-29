import logging
from nose.tools import *

from dbsync.lang import *
from dbsync import models, core, client
from dbsync.client.compression import (
    compress,
    compressed_operations,
    unsynched_objects)

from tests.models import A, B, Base, Session


def addstuff():
    a1 = A(name="first a")
    a2 = A(name="second a")
    b1 = B(name="first b", a=a1)
    b2 = B(name="second b", a=a1)
    b3 = B(name="third b", a=a2)
    session = Session()
    session.add_all([a1, a2, b1, b2, b3])
    session.commit()

def changestuff():
    session = Session()
    a1, a2 = session.query(A)
    b1, b2, b3 = session.query(B)
    a1.name = "first a modified"
    b2.a = a2
    session.delete(b3)
    session.commit()

def setup():
    pass

@core.with_listening(False)
def teardown():
    session = Session()
    map(session.delete, session.query(A))
    map(session.delete, session.query(B))
    map(session.delete, session.query(models.Operation))
    session.commit()


@with_setup(setup, teardown)
def test_tracking():
    addstuff()
    changestuff()
    session = Session()
    assert session.query(models.Operation).\
        filter(models.Operation.command == 'i').\
        count() == 5, "insert operations don't match"
    assert session.query(models.Operation).\
        filter(models.Operation.command == 'u').\
        count() == 2, "update operations don't match"
    assert session.query(models.Operation).\
        filter(models.Operation.command == 'd').\
        count() == 1, "delete operations don't match"


@with_setup(setup, teardown)
def test_compression():
    addstuff()
    changestuff()
    compress() # remove unnecesary operations
    session = Session()
    assert session.query(models.Operation).\
        filter(models.Operation.command == 'i').\
        count() == 4, "insert operations don't match"
    assert session.query(models.Operation).\
        filter(models.Operation.command == 'u').\
        count() == 0, "update operations don't match"
    assert session.query(models.Operation).\
        filter(models.Operation.command == 'd').\
        count() == 0, "delete operations don't match"


@with_setup(setup, teardown)
def test_unsynched_objects_detection():
    addstuff()
    changestuff()
    assert bool(unsynched_objects()), "unsynched objects weren't detected"


@with_setup(setup, teardown)
def test_compression_consistency():
    addstuff()
    changestuff()
    session = Session()
    ops = session.query(models.Operation).all()
    compress()
    news = session.query(models.Operation).order_by(models.Operation.order).all()
    assert news == compressed_operations(ops)


@with_setup(setup, teardown)
def test_compression_correctness():
    addstuff()
    changestuff()
    session = Session()
    ops = compressed_operations(session.query(models.Operation).all())
    groups = group_by(lambda op: (op.content_type_id, op.row_id), ops)
    for g in groups.itervalues():
        logging.info(g)
        assert len(g) == 1
    # assert correctness when compressing operations from a pull
    # message
    pull_ops = [
        models.Operation(command='i', content_type_id=1, row_id=1, order=1),
        models.Operation(command='d', content_type_id=1, row_id=1, order=2),
        models.Operation(command='i', content_type_id=1, row_id=1, order=3),
        models.Operation(command='u', content_type_id=1, row_id=1, order=4),
        # result of above should be a single 'i'
        models.Operation(command='u', content_type_id=2, row_id=1, order=5),
        models.Operation(command='d', content_type_id=2, row_id=1, order=6),
        models.Operation(command='i', content_type_id=2, row_id=1, order=7),
        models.Operation(command='d', content_type_id=2, row_id=1, order=8),
        # result of above should be a single 'd'
        models.Operation(command='d', content_type_id=3, row_id=1, order=9),
        models.Operation(command='i', content_type_id=3, row_id=1, order=10),
        # result of above should be an 'u'
        models.Operation(command='i', content_type_id=4, row_id=1, order=11),
        models.Operation(command='u', content_type_id=4, row_id=1, order=12),
        models.Operation(command='d', content_type_id=4, row_id=1, order=13),
        # result of above should be no operations
        models.Operation(command='d', content_type_id=5, row_id=1, order=14),
        models.Operation(command='i', content_type_id=5, row_id=1, order=15),
        models.Operation(command='d', content_type_id=5, row_id=1, order=16),
        # result of above should be a single 'd'
        models.Operation(command='u', content_type_id=6, row_id=1, order=17),
        models.Operation(command='d', content_type_id=6, row_id=1, order=18),
        models.Operation(command='i', content_type_id=6, row_id=1, order=19),
        # result of above should be an 'u'
        models.Operation(command='d', content_type_id=7, row_id=1, order=20),
        models.Operation(command='i', content_type_id=7, row_id=1, order=21),
        models.Operation(command='u', content_type_id=7, row_id=1, order=22)
        # result of above should be an 'u'
        ]
    compressed = compressed_operations(pull_ops)
    logging.info("len(compressed) == {0}".format(len(compressed)))
    logging.info("\n".join(repr(op) for op in compressed))
    assert len(compressed) == 6
    assert compressed[0].command == 'i'
    assert compressed[1].command == 'd'
    assert compressed[2].command == 'u'
    assert compressed[3].command == 'd'
    assert compressed[4].command == 'u'
    assert compressed[5].command == 'u'
