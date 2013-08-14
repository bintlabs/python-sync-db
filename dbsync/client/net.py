"""
Send HTTP requests and interpret responses.

The body returned by each procedure will be a python dictionary
obtained from parsing a JSON response, or ``None`` if the response
isn't valid JSON.

These procedures will raise a NetworkError in case of network failure.
"""

import socket
import httplib
import urllib
import urlparse
import json


class NetworkError(Exception): pass


_headers = {"Content-Type": "application/json",
            "Accept": "application/json"}


def post_request(server_url, json_dict):
    """Sends a POST request to *server_url* with data *json_dict* and
    returns a trio of (code, reason, body)."""
    if not server_url.startswith("http://") and \
            not server_url.startswith("https://"):
        server_url = "http://" + server_url
    scheme, netloc, path, _, _, _ = urlparse.urlparse(server_url)
    try:
        conn = (httplib.HTTPSConnection if scheme == "https" \
                    else httplib.HTTPConnection)(netloc)
        conn.request("POST", path, json.dumps(json_dict), _headers)
        response = conn.getresponse()
        body = None
        try:
            body = json.loads(response.read())
        except ValueError: pass
        result = (response.status, response.reason, body)
        conn.close()
        return result
    except socket.error as e:
        raise NetworkError(*e.args)


def get_request(server_url, data=None):
    """Sends a GET request to *server_url*. If *data* is to be added,
    it should be a python dictionary with simple pairs suitable for
    url encoding. Returns a trio of (code, reason, body)."""
    if not server_url.startswith("http://") and \
            not server_url.startswith("https://"):
        server_url = "http://" + server_url
    scheme, netloc, path, _, _, _ = urlparse.urlparse(server_url)
    arguments = ("?" + urllib.urlencode(data)) if data is not None else ""
    try:
        conn = (httplib.HTTPSConnection if scheme == "https" \
                    else httplib.HTTPConnection)(netloc)
        conn.request("GET", path + arguments, headers=_headers)
        response = conn.getresponse()
        body = None
        try:
            body = json.loads(response.read())
        except ValueError: pass
        result = (response.status, response.reason, body)
        conn.close()
        return result
    except socket.error as e:
        raise NetworkError(*e.args)
