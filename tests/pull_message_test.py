from nose.tools import *
import datetime
import logging
import json

from dbsync import models
from dbsync.messages.pull import PullMessage

from tests.models import A, B, Base, Session


def addstuff():
    a1 = A(name="first a")
    a2 = A(name="second a")
    b1 = B(name="first b", a=a1)
    b2 = B(name="second b", a=a1)
    b3 = B(name="third b", a=a2)
    session = Session()
    session.add_all([a1, a2, b1, b2, b3])
    version = models.Version()
    version.created = datetime.datetime.now()
    session.add(version)
    session.flush()
    for op in session.query(models.Operation):
        op.version_id = version.version_id
    session.commit()

def setup(): pass

def teardown():
    session = Session()
    map(session.delete, session.query(A))
    map(session.delete, session.query(B))
    map(session.delete, session.query(models.Operation))
    map(session.delete, session.query(models.Version))
    session.commit()


@with_setup(setup, teardown)
def test_create_message():
    addstuff()
    session = Session()
    message = PullMessage()
    version = session.query(models.Version).first()
    message.add_version(version)
    assert message.to_json() == PullMessage(message.to_json()).to_json()


@with_setup(setup, teardown)
def test_encode_message():
    addstuff()
    session = Session()
    message = PullMessage()
    version = session.query(models.Version).first()
    message.add_version(version)
    assert message.to_json() == json.loads(json.dumps(message.to_json()))
