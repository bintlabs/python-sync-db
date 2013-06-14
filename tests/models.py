from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class A(Base):
    __tablename__ = "test_a"

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return u"<A id:{0} name:{1}>".format(self.id, self.name)


class B(Base):
    __tablename__ = "test_b"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    a_id = Column(Integer, ForeignKey("test_a.id"))

    a = relationship(A)

    def __repr__(self):
        return u"<B id:{0} name:{1} a_id:{2}>".format(
            self.id, self.name, self.a_id)
