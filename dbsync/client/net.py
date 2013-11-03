"""
Send HTTP requests and interpret responses.

The body returned by each procedure will be a python dictionary
obtained from parsing a response through a decoder, or ``None`` if the
decoder raises a ``ValueError``. The default encoder, decoder and
headers are meant to work with the JSON spec.

These procedures will raise a NetworkError in case of network failure.
"""

import socket
import httplib
import urllib
import urlparse
import inspect
import json


class NetworkError(Exception): pass


_headers = {"Content-Type": "application/json",
            "Accept": "application/json"}

def _defaults(encode, decode, headers):
    e = encode if not encode is None else json.dumps
    if not inspect.isroutine(e):
        raise ValueError("encoder must be a function", e)
    d = decode if not decode is None else json.loads
    if not inspect.isroutine(d):
        raise ValueError("decoder must be a function", d)
    h = headers if not headers is None else _headers
    if h and not isinstance(h, dict):
        raise ValueError("headers must be False or a python dictionary", h)
    return (e, d, h)


def post_request(server_url, json_dict, encode=None, decode=None, headers=None):
    """Sends a POST request to *server_url* with data *json_dict* and
    returns a trio of (code, reason, body)."""
    if not server_url.startswith("http://") and \
            not server_url.startswith("https://"):
        server_url = "http://" + server_url
    scheme, netloc, path, _, _, _ = urlparse.urlparse(server_url)
    enc, dec, hhs = _defaults(encode, decode, headers)
    try:
        conn = (httplib.HTTPSConnection if scheme == "https" \
                    else httplib.HTTPConnection)(netloc)
        conn.request("POST", path, enc(json_dict), hhs or None)
        response = conn.getresponse()
        body = None
        try:
            body = dec(response.read())
        except ValueError: pass
        result = (response.status, response.reason, body)
        conn.close()
        return result
    except socket.error as e:
        raise NetworkError(*e.args)


def get_request(server_url, data=None, encode=None, decode=None, headers=None):
    """Sends a GET request to *server_url*. If *data* is to be added,
    it should be a python dictionary with simple pairs suitable for
    url encoding. Returns a trio of (code, reason, body)."""
    if not server_url.startswith("http://") and \
            not server_url.startswith("https://"):
        server_url = "http://" + server_url
    scheme, netloc, path, _, _, _ = urlparse.urlparse(server_url)
    arguments = ("?" + urllib.urlencode(data)) if data is not None else ""
    enc, dec, hhs = _defaults(encode, decode, headers)
    try:
        conn = (httplib.HTTPSConnection if scheme == "https" \
                    else httplib.HTTPConnection)(netloc)
        conn.request("GET", path + arguments, headers=hhs or None)
        response = conn.getresponse()
        body = None
        try:
            body = dec(response.read())
        except ValueError: pass
        result = (response.status, response.reason, body)
        conn.close()
        return result
    except socket.error as e:
        raise NetworkError(*e.args)


def head_request(server_url):
    """Sends a HEAD request to *server_url*.

    Returns a pair of (code, reason)."""
    if not server_url.startswith("http://") and \
            not server_url.startswith("https://"):
        server_url = "http://" + server_url
    scheme, netloc, path, _, _, _ = urlparse.urlparse(server_url)
    try:
        conn = (httplib.HTTPSConnection if scheme == "https" \
                    else httplib.HTTPConnection)(netloc)
        conn.request("HEAD", path)
        response = conn.getresponse()
        conn.close()
        return (response.status, response.reason)
    except socket.error as e:
        raise NetworkError(*e.args)
