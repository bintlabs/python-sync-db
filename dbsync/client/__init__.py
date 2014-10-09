"""
Interface for the synchronization client.

The client or node emits 'push' and 'pull' requests to the server. The
client can also request a registry key if it hasn't been given one
yet.
"""

import inspect

from dbsync.client.compression import unsynched_objects, trim
from dbsync.client.tracking import track
from dbsync.core import extend
from dbsync.client.register import (
	register,
	isregistered,
	get_node,
	save_node)
from dbsync.client.pull import UniqueConstraintError, pull
from dbsync.client.push import push
from dbsync.client.ping import isconnected, isready
from dbsync.client.repair import repair
from dbsync.client.serverquery import query_server
from dbsync.client import net


def set_default_encoder(enc):
    assert inspect.isroutine(enc), "encoder must be a function"
    net.default_encoder = enc


def set_default_decoder(dec):
    assert inspect.isroutine(dec), "decoder must be a function"
    net.default_decoder = dec


def set_default_headers(hhs):
    assert isinstance(hhs, dict), "headers must be a dictionary"
    net.default_headers = hhs


def set_default_timeout(t):
    assert isinstance(t, (int, long, float)), "timeout must be a number"
    net.default_timeout = t


def set_authentication_callback(c):
    assert inspect.isroutine(c), "authentication callback must be a function"
    net.authentication_callback = c
