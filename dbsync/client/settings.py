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
    for k in kwargs:
        if k not in _networking_paremeters:
            raise ValueError("unknown configuration parameter: {0}".format(k))
        _networking_parameters = kwargs[k]
    return _networking_parameters
