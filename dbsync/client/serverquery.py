"""
Query the server's database.

For now, only equality filters are allowed, and they will be joined
together by ``and_`` in the server.
"""

from dbsync import core
from dbsync.messages.base import BaseMessage
from dbsync.client.net import get_request


class BadResponseError(Exception): pass


def query_server(query_url, *class_, **filters):
    """Queries the server for a single object's dataset.

    If no class and no filters are given, the procedure returns a
    curried form."""
    def query(cls, encode=None, decode=None, headers=None, **args):
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
    if not class_ and not filters:
        return query
    if len(class_) != 1:
        raise TypeError(
            "query_server takes exactly 1 class argument ({0} given)".\
                format(len(class_)))
    return query(class_[0], **filters)
