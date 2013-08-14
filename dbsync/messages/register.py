"""
Register message and related.
"""

from dbsync.models import Node
from dbsync.utils import properties_dict, object_from_dict
from dbsync.messages.codecs import encode_dict, decode_dict


class RegisterMessage(object):
    """A register message with node information."""

    #: The node to be registered in the client application
    node = None

    def __init__(self, raw_data=None):
        if raw_data is not None:
            self._build_from_raw(raw_data)

    def _build_from_raw(self, data):
        self.node = object_from_dict(Node, decode_dict(Node)(data['node']))

    def to_json(self):
        encoded = {}
        encoded['node'] = None
        if self.node is not None:
            encoded['node'] = encode_dict(Node)(properties_dict(self.node))
        return encoded
