from sqlalchemy import Column, Integer, String, Date, ForeignKey, create_engine
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


engine = create_engine("sqlite:///node.db")
Session = sessionmaker(bind=engine)


Base = declarative_base()


class City(Base):

    __tablename__ = "city"

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return u"<City id: {0}; name: {1}>".format(self.id, self.name)


class House(Base):

    __tablename__ = "house"

    id = Column(Integer, primary_key=True)
    address = Column(String)
    city_id = Column(Integer, ForeignKey("city.id"))

    city = relationship(City, backref="houses")

    def __repr__(self):
        return u"<House id: {0}; address: {1}; city_id: {2}>".format(
            self.id, self.address, self.city_id)


class Person(Base):

    __tablename__ = "person"

    id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    house_id = Column(Integer, ForeignKey("house.id"))
    birth_city_id = Column(Integer, ForeignKey("city.id"))
    birth_date = Column(Date)
    email = Column(String)

    house = relationship(House, backref="persons")
    birth_city = relationship(City)

    def __repr__(self):
        return u"<Person '{0} {1}' house_id: {2}; birth_city_id: {3}>".\
            format(self.first_name,
                   self.last_name,
                   self.house_id,
                   self.birth_city_id)


Base.metadata.create_all(engine)
