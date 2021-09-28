import pathlib  # noqa F401
import os  # noqa F401
import sys  # noqa F401

import logging as log

# Useful imports for notebooks
import pandas as pd  # noqa F401
import keyring  # noqa F401

from os import getenv  # noqa F401
from IPython.display import HTML  # noqa F401
from pprint import pprint  # noqa F401
from getpass import getpass
from datetime import datetime  # noqa F401


def init_jupyter():
    debug = False
    if 'should_debug' in globals():
        debug = True
    enable_logging(debug)


def enable_logging(should_debug=False):
    log_level = log.INFO
    if should_debug is True:
        log_level = log.DEBUG

    log.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=log_level,
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def store_secret(service, username=None):
    '''
    Stores a password in the macOS keychain or Windows Credential Manager.
    '''
    secret = getpass(f'Enter the secret for `{service}` / `{username}`:')

    service_full = f'jupyter_{service}'
    keyring.set_password(
        service_full,
        username,
        secret,
    )


def get_secret(service, username=None):
    '''
    Retrieves a password from the macOS keychain or Windows Credential Manager.

    Allows you to keep passwords outside of jupyter notebooks.
    '''
    service_full = f'jupyter_{service}'
    return keyring.get_password(
        service_full,
        username,
    )
