# Copyright (C) 2019 Michael Lyubinin
# Author: Michael Lyubinin
# Contact: michael@lyubinin.com

""" Misc utility functions used by gconfiglib """

import logging
import collections
import urlparse
import datetime as dt
import pandas as pd
from kazoo.client import KazooClient  # pylint: disable=F0401
from kazoo.security import make_digest_acl  # pylint: disable=F0401
import kazoo.exceptions  # pylint: disable=F0401
import kazoo.handlers.threading  # pylint: disable=F0401

def zk_connect(uri):
    """
    Connects to zookeeper
    :return: zookeeper connection, or None if unsuccessful
    """
    logger = logging.getLogger('gconfiglib')
    zk_uri = urlparse.urlparse(uri)
    if zk_uri.scheme != 'zookeeper':
        logger.log(logging.ERROR, 'Expecting uri in zookeeper://user:passwd@host/prefix format')
        return False
    host = zk_uri.hostname
    credentials = zk_uri.username + ':' + zk_uri.password
    prefix = zk_uri.path
    try:
        zk_conn = KazooClient(hosts=host,
                              default_acl=[make_digest_acl(*credentials.split(':'), all=True)],
                              auth_data=[('digest', credentials)])
        zk_conn.start()
        zk_conn.ensure_path(prefix)
        logger.log(logging.DEBUG, 'Connected to Zookeeper at ' + host)
    except kazoo.handlers.threading.KazooTimeoutError as exception:
        logger.log(logging.ERROR, 'Could not connect to ZooKeeper server, ' + str(exception))
        return None
    return zk_conn


def json_serial(obj):
    """
    JSON serializer for objects not serializable by default json code
    Any additional object types we need to support - add them here
    """

    if isinstance(obj, dt.date) or isinstance(obj, dt.datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError("Type not serializable")


def json_decoder(payload):
    """
    Custom de-serializer for reading JSON files into OrderedDict
    Add custom de-serialization for any additional object types here. E.g., datetime
    :param payload: dict object from json load
    :return: OrderedDict
    """
    payload = collections.OrderedDict(payload)
    for key, value in payload.iteritems():
        if isinstance(value, basestring):
            try:
                # We keep single number as a string, but attempt to convert something that looks like a date
                float(value)
            except ValueError:
                date_value = _to_datetime(value)[0]
                if date_value is not None:
                    payload[key] = date_value.to_pydatetime()
    return payload


def _to_datetime(date_str, fmt=None):
    """
    Attempts to convert string to datetime for specific format,
    and tries to guess format if the specified format fails
    :param date_str: string to convert to datetime
    :param fmt: format to try first
    :return: (datetime, format) - returns both conversion result and format used,
        or (None, None) on failure
    """
    if date_str == '':
        return None, None
    if fmt is None:
        fmt = '%Y-%m-%d %H:%M'
    try:
        date_format = fmt
        dt_res = pd.to_datetime(date_str, format=date_format, exact=False)
    except ValueError:
        try:
            date_format = '%Y-%m-%d %H:%M'
            dt_res = pd.to_datetime(date_str, format=date_format, exact=False)
        except ValueError:
            try:
                date_format = '%Y-%m-%d'
                dt_res = pd.to_datetime(date_str, format=date_format, exact=False)
            except ValueError:
                try:
                    date_format = '%m/%d/%Y'
                    dt_res = pd.to_datetime(date_str, format=date_format, exact=False)
                except ValueError:
                    try:
                        date_format = '%m-%d-%Y'
                        dt_res = pd.to_datetime(date_str, format=date_format, exact=False)
                    except ValueError:
                        return None, None
    return dt_res, date_format
