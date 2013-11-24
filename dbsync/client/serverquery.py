"""
Query the server's database.

For now, only equality filters are allowed, and they will be joined
together by ``and_`` in the server.
"""

from dbsync import core
from dbsync.messages.base import BaseMessage
from dbsync.client.net import get_request


class BadResponseError(Exception): pass


def query_server(query_url,
                 encode=None, decode=None, headers=None, monitor=None):
    """Queries the server for a single model's dataset.

    This procedure returns a procedure that receives the class and
    filters, and performs the HTTP request."""
    def query(cls, **args):
        data = {'model': cls.__name__}
        data.update(dict(('{0}_{1}'.format(cls.__name__, key), value)
                         for key, value in args.iteritems()))

        code, reason, response = get_request(
            query_url, data, encode, decode, headers, monitor)

        if (code // 100 != 2):
            if monitor: monitor({'status': "error", 'reason': reason.lower()})
            raise BadResponseError(code, reason, response)
        if response is None:
            if monitor: monitor({'status': "error",
                                 'reason': "invalid response format"})
            raise BadResponseError(code, reason, response)
        message = None
        try:
            message = BaseMessage(response)
        except KeyError:
            if monitor: monitor({'status': "error",
                                 'reason': "invalid message format"})
            raise BadResponseError(
                "response object isn't a valid BaseMessage", response)
        return message.query(cls).all()
    return query
