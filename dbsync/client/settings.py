"""
Client library configuration.
"""


#: networking configuration
_networking_parameters = {
    "server_url": "localhost",
    "encrypt": False, # specific algorithm
    }


def configure_networking(**kwargs):
    """Configurate the networking aspect of the client library.

    Documentation pending"""
    for k, v in kwargs.iteritems():
        if k not in _networking_parameters:
            raise ValueError("unknown configuration parameter: {0}".format(k))
        _networking_parameters[k] = v
    return _networking_parameters
