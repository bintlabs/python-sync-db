"""
Send HTTP requests and interpret responses.

The body returned by each procedure will be a python dictionary
obtained from parsing a response through a decoder, or ``None`` if the
decoder raises a ``ValueError``. The default encoder, decoder and
headers are meant to work with the JSON specification.

These procedures will raise a NetworkError in case of network failure.
"""

import requests
import cStringIO
import inspect
import json


class NetworkError(Exception):
    pass


default_encoder = json.dumps

default_decoder = json.loads

default_headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}

default_timeout = 1


def _defaults(encode, decode, headers):
    e = encode if not encode is None else default_encoder
    if not inspect.isroutine(e):
        raise ValueError("encoder must be a function", e)
    d = decode if not decode is None else default_decoder
    if not inspect.isroutine(d):
        raise ValueError("decoder must be a function", d)
    h = headers if not headers is None else default_headers
    if h and not isinstance(h, dict):
        raise ValueError("headers must be False or a python dictionary", h)
    return (e, d, h)


def post_request(server_url, json_dict,
                 encode=None, decode=None, headers=None,
                 monitor=None):
    """
    Sends a POST request to *server_url* with data *json_dict* and
    returns a trio of (code, reason, body).

    *encode* is a function that transforms a python dictionary into a
    string.

    *decode* is a function that transforms a string into a python
    dictionary.

    For all dictionaries d of simple types, decode(encode(d)) == d.

    *headers* is a python dictionary with headers to send.

    *monitor* is a routine that gets called for each chunk of the
    response received, and is given two arguments: the size of the
    response in bytes, and the current amount received. If without
    issue, *monitor* should receive the pair (size, 0) at first, and
    the pair (size, size) when finished. The size will be ``None`` if
    it's unknown, in which case the final pair would be (None,
    actual_size).
    """
    if not server_url.startswith("http://") and \
            not server_url.startswith("https://"):
        server_url = "http://" + server_url
    enc, dec, hhs = _defaults(encode, decode, headers)
    stream = inspect.isroutine(monitor)
    try:
        r = requests.post(server_url, data=enc(json_dict),
                          headers=hhs or None, stream=stream,
                          timeout=default_timeout)
        response = None
        if stream:
            total = r.headers.get('content-length', None)
            partial = 0
            monitor({'status': "connect", 'size': total})
            chunks = cStringIO.StringIO()
            for chunk in r:
                partial += len(chunk)
                monitor({'status': "downloading",
                         'size': total, 'received': partial})
                chunks.write(chunk)
            response = chunks.getvalue()
            chunks.close()
        else:
            response = r.content
        body = None
        try:
            body = dec(response)
        except ValueError:
            pass
        result = (r.status_code, r.reason, body)
        r.close()
        return result

    except requests.exceptions.RequestException as e:
        if stream:
            monitor({'status': "error", 'reason': "network error"})
        raise NetworkError(*e.args)

    except Exception as e:
        if stream:
            monitor({'status': "error", 'reason': "network error"})
        raise NetworkError(*e.args)


def get_request(server_url, data=None,
                encode=None, decode=None, headers=None,
                monitor=None):
    """
    Sends a GET request to *server_url*. If *data* is to be added, it
    should be a python dictionary with simple pairs suitable for url
    encoding. Returns a trio of (code, reason, body).

    Read the docstring for ``post_request`` for information on the
    rest.
    """
    if not server_url.startswith("http://") and \
            not server_url.startswith("https://"):
        server_url = "http://" + server_url
    enc, dec, hhs = _defaults(encode, decode, headers)
    stream = inspect.isroutine(monitor)
    try:
        r = requests.get(server_url, params=data,
                         headers=hhs or None, stream=stream,
                         timeout=default_timeout)
        response = None
        if stream:
            total = r.headers.get('content-length', None)
            partial = 0
            monitor({'status': "connect", 'size': total})
            chunks = cStringIO.StringIO()
            for chunk in r:
                partial += len(chunk)
                monitor({'status': "downloading",
                         'size': total, 'received': partial})
                chunks.write(chunk)
            response = chunks.getvalue()
            chunks.close()
        else:
            response = r.content
        body = None
        try:
            body = dec(response)
        except ValueError:
            pass
        result = (r.status_code, r.reason, body)
        r.close()
        return result

    except requests.exceptions.RequestException as e:
        if stream:
            monitor({'status': "error", 'reason': "network error"})
        raise NetworkError(*e.args)

    except Exception as e:
        if stream:
            monitor({'status': "error", 'reason': "network error"})
        raise NetworkError(*e.args)


def head_request(server_url):
    """
    Sends a HEAD request to *server_url*.

    Returns a pair of (code, reason).
    """
    if not server_url.startswith("http://") and \
            not server_url.startswith("https://"):
        server_url = "http://" + server_url
    try:
        r = requests.head(server_url, timeout=default_timeout)
        return (r.status_code, r.reason)

    except requests.exceptions.RequestException as e:
        raise NetworkError(*e.args)

    except Exception as e:
        raise NetworkError(*e.args)
