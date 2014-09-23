from sqlalchemy import Column, Integer, String, Date, ForeignKey, create_engine
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

import dbsync
from dbsync import server


engine = create_engine("mysql://root:11235813@localhost/dbsync_apptest", echo=True)
Session = sessionmaker(bind=engine)


dbsync.set_engine(engine)


Base = declarative_base()
Base.__table_args__ = {'sqlite_autoincrement': True,}


@server.track
class City(Base):

    __tablename__ = "city"

    id = Column(Integer, primary_key=True)
    name = Column(String(500))

    def __repr__(self):
        return u"<City id: {0}; name: {1}>".format(self.id, self.name)

name_pool = ["foo", "bar", "baz"]

def load_extra(city):
    return "-".join(name_pool) + "-" + city.name

def save_extra(city, data):
    print "SAVING -------------------"
    print city.name, data
    print "SAVED  -------------------"

def delete_extra(old_city, new_city):
    print "DELETING -----------------"
    print old_city.name, (new_city.name if new_city is not None else None)
    print "DELETED  -----------------"

server.extend(City, "extra", String, load_extra, save_extra, delete_extra)


@server.track
class House(Base):

    __tablename__ = "house"

    id = Column(Integer, primary_key=True)
    address = Column(String(500), unique=True)
    city_id = Column(Integer, ForeignKey("city.id"))

    city = relationship(City, backref="houses")

    def __repr__(self):
        return u"<House id: {0}; address: {1}; city_id: {2}>".format(
            self.id, self.address, self.city_id)


@server.track
class Person(Base):

    __tablename__ = "person"

    __table_args__ = (UniqueConstraint('first_name', 'last_name'),
                      Base.__table_args__)

    id = Column(Integer, primary_key=True)
    first_name = Column(String(500))
    last_name = Column(String(500))
    house_id = Column(Integer, ForeignKey("house.id"))
    birth_city_id = Column(Integer, ForeignKey("city.id"))
    birth_date = Column(Date)
    email = Column(String(500))

    house = relationship(House, backref="persons")
    birth_city = relationship(City)

    def __repr__(self):
        return u"<Person '{0} {1}' house_id: {2}; birth_city_id: {3}>".\
            format(self.first_name,
                   self.last_name,
                   self.house_id,
                   self.birth_city_id)


Base.metadata.create_all(engine)
dbsync.create_all()
dbsync.generate_content_types()
