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
import dbsync.client.push
from dbsync.client.push import PushRejected, PullSuggested, push
from dbsync.client.ping import isconnected, isready
from dbsync.client.repair import repair
from dbsync.client.serverquery import query_server
from dbsync.client import net


def set_pull_suggestion_criterion(predicate):
    """
    Sets the predicate used to check whether a push response suggests
    the node that a pull should be performed. Default value is a
    constant ``False`` procedure.

    If set, it should be a procedure that receives three arguments: an
    HTTP code, the HTTP reason for said code, and the response (a
    dictionary). If for a given response of HTTP return code not in
    the 200s the procedure returns ``True``, the PullSuggested
    exception will be raised. PullSuggested inherits from
    PushRejected.
    """
    assert inspect.isroutine(predicate), "criterion must be a function"
    dbsync.client.push.suggests_pull = predicate
    return predicate


def set_default_encoder(enc):
    """
    Sets the default encoder used to encode simplified dictionaries to
    strings, the messages being sent to the server. Default is
    json.dumps
    """
    assert inspect.isroutine(enc), "encoder must be a function"
    net.default_encoder = enc
    return enc


def set_default_decoder(dec):
    """
    Sets the default decoder used to decode strings, the messages
    received from the server, into the dictionaries interpreted by the
    library. Default is json.loads
    """
    assert inspect.isroutine(dec), "decoder must be a function"
    net.default_decoder = dec
    return dec


def set_default_headers(hhs):
    """
    Sets the default headers sent in HTTP requests. Default is::

        {"Content-Type": "application/json",
         "Accept": "application/json"}
    """
    assert isinstance(hhs, dict), "headers must be a dictionary"
    net.default_headers = hhs


def set_default_timeout(t):
    """
    Sets the default timeout in seconds for all HTTP requests. Default
    is 10
    """
    assert isinstance(t, (int, long, float)), "timeout must be a number"
    net.default_timeout = t


def set_authentication_callback(c):
    """
    Sets a procedure that returns an authentication object, used in
    POST and GET requests. The procedure should receive the url of the
    request, and return an object according to
    http://docs.python-requests.org/en/latest/user/authentication/
    """
    assert inspect.isroutine(c), "authentication callback must be a function"
    net.authentication_callback = c
    return c
