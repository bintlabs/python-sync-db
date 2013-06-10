from nose.tools import *


def setup():
    print "SETUP!"


def teardown():
    print "TEAR DOWN!"


@with_setup(setup, teardown)
def test_things():
    print "THINGS TESTED!"
