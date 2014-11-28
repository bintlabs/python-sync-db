"""
.. module:: messages.codecs
   :synopsis: Encoding and decoding of specific datatypes.
"""

import datetime
import base64
import decimal

from sqlalchemy import types
from dbsync import core
from dbsync.lang import *
from dbsync.utils import types_dict as bare_types_dict


def types_dict(class_):
    "Augments standard types_dict with model extensions."
    dict_ = bare_types_dict(class_)
    extensions = core.model_extensions.get(class_.__name__, {})
    for field, ext in extensions.iteritems():
        type_, _, _, _ = ext
        dict_[field] = type_
    return dict_


def _encode_table(type_):
    "*type_* is a SQLAlchemy data type."
    if isinstance(type_, types.Date):
        return lambda value: [value.year, value.month, value.day]
    elif isinstance(type_, types.DateTime):
        return lambda value: [value.year, value.month, value.day,
                              value.hour, value.minute, value.second,
                              value.microsecond]
    elif isinstance(type_, types.Time):
        return lambda value: [value.hour, value.minute, value.second,
                              value.microsecond]
    elif isinstance(type_, types.LargeBinary):
        return base64.standard_b64encode
    elif isinstance(type_, types.Numeric):
        return str
    return identity

#: Encodes a python value into a JSON-friendly python value.
encode = lambda t: guard(_encode_table(t))

def encode_dict(class_):
    """
    Returns a function that transforms a dictionary, mapping the
    types to simpler ones, according to the given mapped class.
    """
    types = types_dict(class_)
    encodings = dict((k, encode(t)) for k, t in types.iteritems())
    return lambda dict_: dict((k, encodings[k](v))
                              for k, v in dict_.iteritems()
                              if k in encodings)


def _decode_table(type_):
    "*type_* is a SQLAlchemy data type."
    if isinstance(type_, types.Date):
        return partial(apply, datetime.date)
    elif isinstance(type_, types.DateTime):
        return partial(apply, datetime.datetime)
    elif isinstance(type_, types.Time):
        return partial(apply, datetime.time)
    elif isinstance(type_, types.LargeBinary):
        return base64.standard_b64decode
    elif isinstance(type_, types.Numeric):
        return decimal.Decimal
    return identity

#: Decodes a value coming from a JSON string into a richer python value.
decode = lambda t: guard(_decode_table(t))

def decode_dict(class_):
    """
    Returns a function that transforms a dictionary, mapping the
    types to richer ones, according to the given mapped class.
    """
    types = types_dict(class_)
    decodings = dict((k, decode(t)) for k, t in types.iteritems())
    return lambda dict_: dict((k, decodings[k](v))
                              for k, v in dict_.iteritems()
                              if k in decodings)
