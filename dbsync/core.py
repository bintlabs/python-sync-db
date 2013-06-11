"""
Common functionality for model synchronization and version tracking.
"""

from dbsync.models import ContentType


#: List of classes marked for synchronization and change tracking.
synched_models = []


#: Toggled variable to disable listening to operations momentarily.
listening = True


def toggle_listening(enabled=None):
    """Change the listening state.

    If set to ``False``, no operations will be registered."""
    global listening
    listening = enabled if enabled is not None else not listening


def generate_content_types(session=None):
    """Inserts content types into the internal table used to describe
    operations."""
    if session is None:
        raise TypeError("you must provide a valid session")
    for model in synched_models:
        tname = model.__table__.name
        if session.query(ContentType).\
                filter(ContentType.table_name == tname).count() == 0:
            session.add(ContentType(table_name=tname))
    session.flush()
