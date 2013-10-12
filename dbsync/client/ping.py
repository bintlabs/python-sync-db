"""
Ping the server.

The ping procedures are used to quickly diagnose the internet
connection and server status from the client application.
"""

from dbsync.client.net import head_request, NetworkError


def is_connected(ping_url):
    """Whether the client application is connected to the Internet."""
    try:
        head_request(ping_url)
        return True
    except NetworkError:
        return False


def server_ready(ping_url):
    """Whether the server is ready to receive synchronization
    requests."""
    try:
        code, reason = head_request(ping_url)
        return code // 100 == 2
    except:
        return False
