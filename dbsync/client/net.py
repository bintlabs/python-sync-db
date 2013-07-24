"""
Send HTTP requests and interpret responses.

Sent and received data is expected to be a JSON string.

These procedures will raise a NetworkError in case of network failure.
"""

import socket
import httplib
import urllib
import json


class NetworkError(Exception): pass


_headers = {"Content-Type": "application/json",
            "Accept": "application/json"}


def _parsed_selector(selector):
    """Should probably let a library handle this kind of stuff."""
    if not selector: return ""
    parsed = selector
    if not selector.startswith("/"):
        parsed = "/" + parsed
    # if not selector.endswith("/"):
    #     parts = selector.split("/")
    #     if "." not in parts[-1]:
    #         parsed += "/"
    return parsed


def post_request(server_url, selector_url, json_dict):
    """Sends a POST request to *<server_url>/<selector_url>* with data
    *json_dict* and returns a trio of (code, reason, body)."""
    try:
        conn = httplib.HTTPConnection(server_url)
        conn.request("POST",
                     _parsed_selector(selector_url),
                     json.dumps(json_dict),
                     _headers)
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


def get_request(server_url, selector_url, data=None):
    """Sends a GET request to *<server_url>/<selector_url>*. If *data*
    is to be added, it should be a python dictionary with simple pairs
    suitable for url encoding. Returns a trio of (code, reason,
    body)."""
    arguments = ("?" + urllib.urlencode(data)) if data is not None else ""
    try:
        conn = httplib.HTTPConnection(server_url)
        conn.request("GET",
                     _parsed_selector(selector_url) + arguments,
                     headers=_headers)
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
