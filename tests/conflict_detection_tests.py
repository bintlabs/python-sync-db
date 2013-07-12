import logging
from nose.tools import *

from dbsync import models
from dbsync.messages.pull import PullMessage
from dbsync.client.conflicts import (
    find_direct_conflicts,
    find_dependency_conflicts)

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

def create_fake_operations():
    return [models.Operation(row_id=3, content_type_id=2, command='u'), # b3
            models.Operation(row_id=1, content_type_id=1, command='d'), # a1
            models.Operation(row_id=2, content_type_id=1, command='d')] # a2

def setup():
    pass

def teardown():
    session = Session()
    map(session.delete, session.query(A))
    map(session.delete, session.query(B))
    map(session.delete, session.query(models.Operation))
    session.commit()


@with_setup(setup, teardown)
def test_find_direct_conflicts():
    addstuff()
    changestuff()
    session = Session()
    message_ops = create_fake_operations()
    conflicts = find_direct_conflicts(
        session.query(models.Operation).all(), message_ops)
    expected = [
        (message_ops[0],
         models.Operation(row_id=3, content_type_id=2, command='d')), # b3
        (message_ops[1],
         models.Operation(row_id=1, content_type_id=1, command='u')), # a1
        (message_ops[2],
         models.Operation(row_id=2, content_type_id=1, command='u'))] # a2
    logging.info(conflicts)
    logging.info(expected)
    assert repr(conflicts) == repr(expected)


@with_setup(setup, teardown)
def test_find_dependency_conflicts():
    addstuff()
    changestuff()
    session = Session()
    content_types = session.query(models.ContentType).all()
    message_ops = create_fake_operations()
    conflicts = find_dependency_conflicts(
        session.query(models.Operation).all(),
        message_ops,
        content_types,
        session)
    expected = [
        (message_ops[1],
         models.Operation(row_id=1, content_type_id=2, command='i')),
        (message_ops[2],
         models.Operation(row_id=2, content_type_id=2, command='i')),
        (message_ops[2],
         models.Operation(row_id=2, content_type_id=2, command='u'))]
    logging.info(conflicts)
    logging.info(expected)
    assert repr(conflicts) == repr(expected)
