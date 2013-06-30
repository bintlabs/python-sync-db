"""
Encoding and decoding of specific datatypes.
"""

import datetime

from sqlalchemy import types
from dbsync.lang import *
from dbsync.utils import types_dict


def _encode_table(type_):
    """*type_* is a SQLAlchemy data type."""
    if isinstance(type_, types.Date) or isinstance(type_, types.DateTime):
        # TODO encode dates sensibly
        # time will be lost, only the date remains
        return method("toordinal")
    elif isinstance(type_, types.LargeBinary):
        # TODO encode in base64
        return identity
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
        # TODO decode dates sensibly
        return datetime.date.fromordinal
    elif or isinstance(type_, types.DateTime):
        # TODO decode datetimes sensibly
        return datetime.datetime.fromordinal
    elif isinstance(type_, types.LargeBinary):
        # TODO decode from base64
        return identity
    return identity

#: Decodes a value coming from a JSON string into a richer python value.
decode = lambda t: guard(_encode_table(t))

def decode_dict(class_):
    """Returns a function that transforms a dictionary, mapping the
    types to richer ones, according to the given mapped class."""
    types = types_dict(class_)
    decodings = dict((k, decode(t)) for k, t in types.iteritems())
    return lambda dict_: dict((k, decodings[k](v)) for k, v in dict_.iteritems())
