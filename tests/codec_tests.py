from nose.tools import *
import datetime

from dbsync.messages.codecs import encode, encode_dict, decode, decode_dict
from sqlalchemy import types


def test_encode_date():
    today = datetime.date.today()
    e = encode(types.Date())
    d = decode(types.Date())
    assert today == d(e(today))


def test_encode_datetime():
    now = datetime.datetime.now()
    e = encode(types.DateTime())
    d = decode(types.DateTime())
    # microseconds are lost, but that's ok
    assert now.timetuple()[:6] == d(e(now)).timetuple()[:6]
