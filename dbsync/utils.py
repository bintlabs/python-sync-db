"""
Utility functions.
"""

import random


def generate_secret(length=128):
    chars = "0123456789"\
        "abcdefghijklmnopqrstuvwxyz"\
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"\
        ".,_-+*@:;[](){}~!?'|<>=/\&%$#"
    return "".join(random.choice(chars) for _ in xrange(length))
