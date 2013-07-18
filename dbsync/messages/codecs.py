"""
Encoding and decoding of specific datatypes.
"""

import datetime
import time
import base64

from sqlalchemy import types
from dbsync.lang import *
from dbsync.utils import types_dict


def _encode_table(type_):
    """*type_* is a SQLAlchemy data type."""
    if isinstance(type_, types.Date):
        return method("toordinal")
    elif isinstance(type_, types.DateTime):
        return lambda value: time.mktime(value.timetuple())
    elif isinstance(type_, types.LargeBinary):
        return base64.standard_b64encode
    return identity

#: Encodes a python value into a JSON-friendly python value.
encode = lambda t: guard(_encode_table(t))

def encode_dict(class_):
    """Returns a function that transforms a dictionary, mapping the
    types to simpler ones, according to the given mapped class."""
    types = types_dict(class_)
    encodings = dict((k, encode(t)) for k, t in types.iteritems())
    return lambda dict_: dict((k, encodings[k](v)) for k, v in dict_.iteritems())


def _decode_table(type_):
    """*type_* is a SQLAlchemy data type."""
    if isinstance(type_, types.Date):
        return datetime.date.fromordinal
    elif isinstance(type_, types.DateTime):
        return datetime.datetime.fromtimestamp
    elif isinstance(type_, types.LargeBinary):
        return base64.standard_b64decode
    return identity

#: Decodes a value coming from a JSON string into a richer python value.
decode = lambda t: guard(_decode_table(t))

def decode_dict(class_):
    """Returns a function that transforms a dictionary, mapping the
    types to richer ones, according to the given mapped class."""
    types = types_dict(class_)
    decodings = dict((k, decode(t)) for k, t in types.iteritems())
    return lambda dict_: dict((k, decodings[k](v)) for k, v in dict_.iteritems())
