import logging
from nose.tools import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dbsync import models, core, client

from tests.models import A, B, Base


engine = create_engine("sqlite://")
Session = sessionmaker(bind=engine)


def setup():
    client.track(A)
    client.track(B)
    Base.metadata.create_all(engine)
    models.Base.metadata.create_all(engine)
    core.set_engine(engine)
    core.generate_content_types()


def teardown():
    pass


@with_setup(setup, teardown)
def test_tracking():
    a1 = A(name="first a")
    a2 = A(name="second a")
    b1 = B(name="first b", a=a1)
    b2 = B(name="second b", a=a1)
    b3 = B(name="third b", a=a2)
    session = Session()
    session.add_all([a1, a2, b1, b2, b3])
    session.flush()
    a1.name = "first a modified"
    b2.a = a2
    session.delete(b3)
    session.commit()
    for op in session.query(models.Operation):
        logging.info(op)
    assert False
