"""
Top-level exports, for convenience.
"""

import dbsync.core
from dbsync.core import (
    is_synched,
    generate_content_types,
    set_engine,
    get_engine,
    save_extensions)
from dbsync.models import Base
from dbsync.logs import set_log_target


def create_all():
    "Issues DDL commands."
    Base.metadata.create_all(get_engine())


def drop_all():
    "Issues DROP commands."
    Base.metadata.drop_all(get_engine())


def set_listening_mutex(m):
    """
    Sets the internal context manager used to lock sensitive
    operations. It may be a context manager object (e.g. a Lock), a
    class with an empty constructor, or a procedure that can be called
    without arguments and that returns a context manager. In
    multi-threaded applications, using a single mutex to enqueue
    transactions and dbsync's internal operations is recommended.

    Example of mutex that throws an exception on acquisition failure::

        import contextlib
        import functools
        import threading
        import dbsync

        @contextlib.contextmanager
        def acquire(mutex):
            if not mutex.acquire(False):
                raise Exception("couldn't acquire mutex", mutex)
            try: yield
            finally: mutex.release()

        lock = threading.Lock()
        dbsync.set_listening_mutex(functools.partial(acquire, lock))

    Example of mutex that blocks until acquisition::

        import threading
        import dbsync

        lock = threading.Lock()
        dbsync.set_listening_mutex(lock)
    """
    dbsync.core.listening.mutex = m
