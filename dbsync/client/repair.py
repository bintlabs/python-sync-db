"""
Repair client database.

The repair consists in fetching the server database and inserting it
locally while discarding the client database.

Ideally this procedure won't ever be needed. If the synchronization
models get corrupted, either by external interference or simply poor
conflict resolution, further synchronization operations would fail and
be unable to recover. For those cases, a repair operation will restore
the client database to the server state, which is assumed to be
correct.

This procedure can take a long time to complete, since it clears the
client database and fetches a big message from the server.
"""

from dbsync import core
from dbsync.models import Operation, Version
from dbsync.messages.base import BaseMessage
from dbsync.client.net import get_request


@core.with_listening(False)
@core.with_transaction
def repair_database(message, latest_version_id, session=None):
    if not isinstance(message, BaseMessage):
        raise TypeError("need an instance of dbsync.messages.base.BaseMessage "\
                            "to perform the repair operation")
    # clear local database
    for model in core.synched_models.itervalues():
        session.query(model).delete(synchronize_session=False)
    # clear the local operations and versions
    session.query(Operation).delete(synchronize_session=False)
    session.query(Version).delete(synchronize_session=False)
    session.expire_all()
    # load the fetched database
    for modelkey in core.synched_models:
        for obj in message.query(modelkey):
            session.add(obj)
    # load the new version, if any
    if latest_version_id is not None:
        session.add(Version(version_id=latest_version_id))


class BadResponseError(Exception): pass


def repair(repair_url, extra_data=None):
    """Fetches the server database and replaces the local one with it.

    *extra_data* can be used to add user credentials."""
    assert isinstance(repair_url, basestring), "repair url must be a string"
    assert bool(repair_url), "repair url can't be empty"
    if extra_data is not None:
        assert isinstance(extra_data, dict), "extra data must be a dictionary"
    data = extra_data if extra_data is not None else {}

    code, reason, response = get_request(repair_url, data)

    if (code // 100 != 2) or response is None:
        raise BadResponseError(code, reason, response)
    message = None
    try:
        message = BaseMessage(response)
    except KeyError:
        raise BadResponseError(
            "response object isn't a valid BaseMessage", response)
    repair_database(message, response.get("latest_version_id", None))
    return response
