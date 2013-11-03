"""
Query the server's database.

For now, only equality filters are allowed, and they will be joined
together by ``and_`` in the server.
"""

from dbsync import core
from dbsync.messages.base import BaseMessage
from dbsync.client.net import get_request


class BadResponseError(Exception): pass


def query_server(query_url, encode=None, decode=None, headers=None):
    """Queries the server for a single model's dataset.

    This procedure returns a procedure that receives the class and
    filters, and performs the HTTP request."""
    def query(cls, **args):
        data = {'model': cls.__name__}
        data.update(dict(('{0}_{1}'.format(cls.__name__, key), value)
                         for key, value in args.iteritems()))

        code, reason, response = get_request(
            query_url, data, encode, decode, headers)

        if (code // 100 != 2) or response is None:
            raise BadResponseError(code, reason, response)
        message = None
        try:
            message = BaseMessage(response)
        except KeyError:
            raise BadResponseError(
                "response object isn't a valid BaseMessage", response)
        return message.query(cls).all()
    return query
