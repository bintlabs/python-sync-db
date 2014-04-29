import logging
from nose.tools import *

from dbsync import models, core
from dbsync.messages.pull import PullMessage
from dbsync.client.conflicts import (
    find_direct_conflicts,
    find_dependency_conflicts)

from tests.models import A, B, Base, Session

def get_content_type_ids():
    session = Session()
    ct_a = session.query(models.ContentType).filter_by(model_name='A').first()
    ct_b = session.query(models.ContentType).filter_by(model_name='B').first()
    return (ct_a.content_type_id, ct_b.content_type_id)

ct_a_id, ct_b_id = get_content_type_ids()


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
    return [models.Operation(row_id=3, content_type_id=ct_b_id, command='u'),
            models.Operation(row_id=1, content_type_id=ct_a_id, command='d'),
            models.Operation(row_id=2, content_type_id=ct_a_id, command='d')]

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
def test_find_direct_conflicts():
    addstuff()
    changestuff()
    session = Session()
    message_ops = create_fake_operations()
    conflicts = find_direct_conflicts(
        message_ops, session.query(models.Operation).all())
    expected = [
        (message_ops[0],
         models.Operation(row_id=3, content_type_id=ct_b_id, command='d')), # b3
        (message_ops[1],
         models.Operation(row_id=1, content_type_id=ct_a_id, command='u'))] # a1
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
        message_ops,
        session.query(models.Operation).all(),
        content_types,
        session)
    expected = [
        (message_ops[1], # a1
         models.Operation(row_id=1, content_type_id=ct_b_id, command='i')), # b1
        (message_ops[2], # a2
         models.Operation(row_id=2, content_type_id=ct_b_id, command='i')), # b2
        (message_ops[2], # a2
         models.Operation(row_id=2, content_type_id=ct_b_id, command='u'))] # b2
    logging.info(conflicts)
    logging.info(expected)
    assert repr(conflicts) == repr(expected)
