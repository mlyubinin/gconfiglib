# Copyright (C) 2019 Michael Lyubinin
# Author: Michael Lyubinin
# Contact: michael@lyubinin.com

""" Misc utility functions used by gconfiglib."""

import collections
import datetime as dt
import logging
from typing import Any, Optional, Tuple
from urllib import parse as urlparse

import kazoo.exceptions
import kazoo.handlers.threading
import pandas as pd
from kazoo.client import KazooClient
from kazoo.security import make_digest_acl

logger = logging.getLogger(__name__)


def zk_connect(uri: str) -> Optional[KazooClient]:
    """
    Connects to zookeeper
    :return: zookeeper connection, or None if unsuccessful
    """
    zk_uri: urlparse.ParseResult = urlparse.urlparse(uri)
    if (
        zk_uri.scheme != "zookeeper"
        or zk_uri.hostname is None
        or zk_uri.username is None
        or zk_uri.password is None
    ):
        logger.error("Expecting uri in zookeeper://user:passwd@host/prefix format")
        return None
    host: str = zk_uri.hostname
    credentials: str = zk_uri.username + ":" + zk_uri.password
    prefix: str = zk_uri.path
    try:
        zk_conn = KazooClient(
            hosts=host,
            default_acl=[make_digest_acl(*credentials.split(":"), all=True)],
            auth_data=[("digest", credentials)],
        )
        zk_conn.start()
        zk_conn.ensure_path(prefix)
        logger.debug("Connected to Zookeeper at %s", host)
    except kazoo.handlers.threading.KazooTimeoutError as exception:
        logger.exception("Could not connect to ZooKeeper server, %s", exception)
        return None
    return zk_conn


def json_serial(obj: dt.date | dt.datetime) -> str:
    """
    JSON serializer for objects not serializable by default json code
    Any additional object types we need to support - add them here
    """

    # if isinstance(obj, dt.date) or isinstance(obj, dt.datetime):
    serial: str = obj.isoformat()
    return serial
    # raise TypeError("Type not serializable")


def json_decoder(payload: Any) -> collections.OrderedDict[str, Any]:
    """
    Custom de-serializer for reading JSON files into OrderedDict
    Add custom de-serialization for any additional object types here. E.g., datetime
    :param payload: dict object from json load
    :return: OrderedDict
    """
    payload = collections.OrderedDict(payload)
    for key, value in payload.items():
        if isinstance(value, str):
            try:
                # We keep single number as a string, but attempt to convert something that looks like a date
                float(value)
            except ValueError:
                date_value: Optional[dt.datetime] = _to_datetime(value)[0]
                if date_value is not None:
                    payload[key] = date_value
    return payload


def _to_datetime(
    date_str: str, fmt: Optional[str] = None
) -> Tuple[Optional[dt.datetime], Optional[str]]:
    """
    Attempts to convert string to datetime for specific format,
    and tries to guess format if the specified format fails
    :param date_str: string to convert to datetime
    :param fmt: format to try first
    :return: (datetime, format) - returns both conversion result and format used,
        or (None, None) on failure
    """
    if date_str == "":
        return None, None
    if fmt is None:
        fmt = "%Y-%m-%d %H:%M"
    try:
        date_format = fmt
        dt_res = pd.to_datetime(
            date_str, format=date_format, exact=False
        ).to_pydatetime()
    except ValueError:
        try:
            date_format = "%Y-%m-%d %H:%M"
            dt_res = pd.to_datetime(
                date_str, format=date_format, exact=False
            ).to_pydatetime()
        except ValueError:
            try:
                date_format = "%Y-%m-%d"
                dt_res = pd.to_datetime(
                    date_str, format=date_format, exact=False
                ).to_pydatetime()
            except ValueError:
                try:
                    date_format = "%m/%d/%Y"
                    dt_res = pd.to_datetime(
                        date_str, format=date_format, exact=False
                    ).to_pydatetime()
                except ValueError:
                    try:
                        date_format = "%m-%d-%Y"
                        dt_res = pd.to_datetime(
                            date_str, format=date_format, exact=False
                        ).to_pydatetime()
                    except ValueError:
                        return None, None
    return dt_res, date_format
