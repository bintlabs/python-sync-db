"""
Pull message and related.
"""

import datetime

from dbsync.utils import properties_dict, object_from_dict
from dbsync.lang import *

from dbsync.core import Session, synched_models
from dbsync.core.models import ContentType, Operation, Version
from dbsync.message.base import ObjectType, MessageQuery


class PullMessage(object):
    """A pull message.

    A pull message can be queried over by version, operation or model,
    and can be filtered multiple times."""

    #: Datetime of creation.
    created = None

    #: List of operations to perform in the node.
    operations = []

    #: List of versions being pulled.
    versions = []

    #: Extra data required to perform the operations.
    payload = {}

    def __init__(self, raw_data=None):
        """*raw_data* must be a python dictionary."""
        if raw_data is not None:
            self._build_from_raw(raw_data)
        else:
            self.created = datetime.datetime.now()

    def _build_from_raw(self, raw_data):
        self.created = datetime.datetime.fromordinal(raw_data['created'])
        self.operations = map(partial(object_from_dict, Operation),
                              raw_data['Operation'])
        self.versions = map(partial(object_from_dict, Version),
                            raw_data['Version'])
        for k, v in ifilter(lambda (k, v): k in synched_models,
                            raw_data['payload'].iteritems()):
            self.payload[k] = map(lambda dict_: ObjectType(k, **dict_), v)

    def query(self, model):
        """Returns a query object for this message."""
        return MessageQuery(
            model,
            dict(self.payload, **{
                    'models.Operation': self.operations,
                    'models.Version': self.versions}))

    def encode(self, extra=None):
        """To JSON-friendly python dictionary. Structure::

            created: date to ordinal,
            Operation: list of operations as JS objects,
            Version: list of versions as JS objects,
            payload: JS object with lists of JS objects mapped to model names

        Additional information can be added with the *extra* argument,
        which should be a python dictionary."""
        # TODO encode the message to JSON
        pass
