import datetime

from sqlalchemy.orm import sessionmaker

from models import City, House, Person, engine
from dbsync import models


Session = sessionmaker(bind=engine)


# CRUD cities

def create_city(**kwargs):
    session = Session()
    city = City()
    for k, v in kwargs.iteritems():
        setattr(city, k, v)
    session.add(city)
    session.commit()


def update_city(id=None, **kwargs):
    session = Session()
    city = session.query(City).filter(City.id == id).one()
    for k, v in kwargs.iteritems():
        setattr(city, k, v)
    session.commit()


def delete_city(id=None):
    session = Session()
    city = session.query(City).filter(City.id == id).one()
    session.delete(city)
    session.commit()


def read_cities():
    session = Session()
    for city in session.query(City): print city


# CRUD houses

def create_house(**kwargs):
    session = Session()
    house = House()
    for k, v in kwargs.iteritems():
        setattr(house, k, v)
    session.add(house)
    session.commit()


def update_house(id=None, **kwargs):
    session = Session()
    house = session.query(House).filter(House.id == id).one()
    for k, v in kwargs.iteritems():
        setattr(house, k, v)
    session.commit()


def delete_house(id=None):
    session = Session()
    house = session.query(House).filter(House.id == id).one()
    session.delete(house)
    session.commit()


def read_houses():
    session = Session()
    for house in session.query(House): print house


# CRUD persons

def create_person(**kwargs):
    session = Session()
    person = Person()
    for k, v in kwargs.iteritems():
        setattr(person, k, v)
    session.commit()


def update_person(id=None, **kwargs):
    session = Session()
    person = session.query(Person).filter(Person.id == id).one()
    for k, v in kwargs.iteritems():
        setattr(person, k, v)
    session.commit()


def delete_person(id=None):
    session = Session()
    person = session.query(Person).filter(Person.id == id).one()
    session.delete(person)
    session.commit()


def read_persons():
    session = Session()
    for person in session.query(Person): print person


# Synch

def read_content_types():
    session = Session()
    for ct in session.query(models.ContentType): print ct


def read_versions():
    session = Session()
    for version in session.query(models.Version): print version


def read_operations():
    session = Session()
    for op in session.query(models.Operation): print op


def read_nodes():
    session = Session()
    for node in session.query(models.Node): print node
