"""
Interface for the synchronization server.

The server listens to 'push' and 'pull' requests and provides a
customizable registry service.
"""

from dbsync.server.tracking import track
from dbsync.core import extend
from dbsync.server.handlers import (
    handle_register,
    handle_pull,
    handle_push,
    handle_repair,
    handle_query)
from dbsync.server.trim import trim
