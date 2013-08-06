import datetime

from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from dbsync.utils import generate_secret
import dbsync
from dbsync import client, models


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

    a = relationship(A, backref="bs")

    def __repr__(self):
        return u"<B id:{0} name:{1} a_id:{2}>".format(
            self.id, self.name, self.a_id)


Base.metadata.create_all(engine)
dbsync.set_engine(engine)
dbsync.create_all()
dbsync.generate_content_types()
_session = Session()
_session.add(
    models.Node(registered=datetime.datetime.now(), secret=generate_secret(128)))
_session.commit()
_session.close()
