import logging
from nose.tools import *

from dbsync.lang import *
from dbsync import models, core, client
from dbsync.client.compression import compress, compressed_operations

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
    b2.a = a2 # this triggers two 'updates', one is superfluous
    session.delete(b3)
    session.commit()

def setup():
    pass

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
        count() == 3, "update operations don't match"
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
    # TODO further correctness assertions
