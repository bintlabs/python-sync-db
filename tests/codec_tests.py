from nose.tools import *
import datetime
import decimal

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


def test_encode_numeric():
    num = decimal.Decimal('3.3')
    e = encode(types.Numeric())
    d = decode(types.Numeric())
    assert num == d(e(num))


def test_encode_float_numeric():
    num = 3.3
    e = encode(types.Numeric(asdecimal=False))
    d = decode(types.Numeric(asdecimal=False))
    assert num == d(e(num))
