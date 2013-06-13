import logging
from nose.tools import *
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from dbsync import models, core, client


engine = create_engine("sqlite://")
Session = sessionmaker(bind=engine)

Base = declarative_base()


@client.track
class A(Base):
    __tablename__ = "test_a"

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return u"<A id:{0} name:{1}>".format(self.id, self.name)


@client.track
class B(Base):
    __tablename__ = "test_b"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    a_id = Column(Integer, ForeignKey("test_a.id"))

    a = relationship(A)

    def __repr__(self):
        return u"<B id:{0} name:{1} a_id:{2}>".format(
            self.id, self.name, self.a_id)


def setup():
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
