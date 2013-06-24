"""
Pull message and related.
"""

import datetime

from dbsync.utils import properties_dict

from dbsync.core import Session
from dbsync.core.models import ContentType, Operation, Version
from dbsync.message.base import ObjectType, MessageQuery


class PullMessage(object):
    """A pull message.

    A pull message can be queried over by version, operation or model,
    and can be filtered multiple times."""

    def __init__(self, raw_data=None):
        """*raw_data* must be a valid JSON object for it to be parsed
        correctly. If encrypted, the decryption should be handled
        elsewhere."""
        if raw_data is not None:
            self._build_from_raw(raw_data)
        else:
            self.created = datetime.datetime.now()
            # TODO build message from synchrnonization database
            self.operations = []
            self.versions = []
            self.payload = {}

    def _build_from_raw(self, raw_data):
        # TODO build message from raw JSON string
        self.created = None
        self.operations = []
        self.versions = []
        self.payload = {}

    def query(self, model):
        """Returns a query object for this message."""
        return MessageQuery(
            model,
            dict(self.payload, **{
                    'models.Operation': self.operations
                    'models.Version': self.versions}))

    def encode(self):
        # TODO encode the message to JSON
        pass
