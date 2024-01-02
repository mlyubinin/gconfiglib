# Copyright (C) 2019 Michael Lyubinin
# Author: Michael Lyubinin
# Contact: michael@lyubinin.com

""" Gconfiglib enhanced configuration library. """

import argparse
import importlib
import json
import logging
import os
from ast import literal_eval
from types import ModuleType
from typing import Callable, Type
from urllib import parse as urlparse

from kazoo.client import KazooClient
from kazoo.security import make_digest_acl

from gconfiglib import utils
from gconfiglib.config_node import ConfigNode
from gconfiglib.config_root import ConfigRoot
from gconfiglib.template_node_base import TemplateNodeBase


def main() -> None:
    """
    Configuration management utility wrapper - collects command-line arguments
    :return:
    """
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="action")
    parser_ls = subparsers.add_parser("ls", help="List configuration content")
    parser_ls.add_argument(
        "source",
        help="file name or zookeeper uri (zookeeper://user:pwd@host:port/path)",
    )
    parser_ls.add_argument(
        "--template",
        help="module:function module that contains template function and function name",
    )
    parser_cp = subparsers.add_parser("cp", help="Copy configuration")
    parser_cp.add_argument(
        "source",
        help="file name or zookeeper uri (zookeeper://user:pwd@host:port/path)",
    )
    parser_cp.add_argument(
        "dest", help="file name or zookeeper uri (zookeeper://user:pwd@host:port/path)"
    )
    parser_cp.add_argument(
        "--template",
        help="module:function module that contains template function and function name",
    )
    parser_cp.add_argument(
        "--force", help="Force overwrite of existing configuration", action="store_true"
    )
    parser_rm = subparsers.add_parser("rm", help="Remove configuration")
    parser_rm.add_argument(
        "source",
        help="file name or zookeeper uri (zookeeper://user:pwd@host:port/path)",
    )
    args: argparse.Namespace = parser.parse_args()
    cfgctl(args)


def cfgctl(args: argparse.Namespace) -> None:
    """
    Configuration management utility. Displays, copies and removes configuration in files and/or Zookeeper
    :param args:
    :return:
    """

    logger = logging.getLogger(__name__)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(threadName)s %(filename)s:%(funcName)s: %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if args.source:
        src: urlparse.ParseResult = urlparse.urlparse(args.source)
        if src.scheme == "zookeeper":
            zk_s = KazooClient(
                hosts=src.hostname,
                default_acl=[make_digest_acl(src.username, src.password, all=True)],
                auth_data=[("digest", f"{src.username}:{src.password}")],
            )
            zk_s.start()
            zk_s.ensure_path(src.password)
            logger.debug(
                "Connected to source Zookeeper at %s:%s", src.hostname, src.port
            )

    if args.action in ["ls", "cp"] and args.source:
        if args.template:
            module: ModuleType = importlib.import_module(  # noqa: F841
                str(args.template).split(":", maxsplit=1)[0]
            )
            template_gen: Callable[[ConfigNode], Type[TemplateNodeBase]] = literal_eval(
                "module." + str(args.template).split(":", maxsplit=2)[1]
            )
            cfg_src = ConfigRoot(args.source, template_gen=template_gen)
        else:
            cfg_src = ConfigRoot(args.source)

    if args.action == "ls":
        print(str(cfg_src))
    elif args.action == "rm" and args.source:
        if src.scheme == "zookeeper":
            if zk_s.exists(src.path):
                zk_s.delete(src.path, recursive=True)
        else:
            os.system("rm -f " + args.source)
    elif args.action == "cp" and args.source and args.dest:
        dest: urlparse.ParseResult = urlparse.urlparse(args.dest)
        if dest.scheme == "zookeeper":
            zk_d = KazooClient(
                hosts=dest.hostname,
                default_acl=[make_digest_acl(dest.username, dest.password, all=True)],
                auth_data=[("digest", f"{dest.username}:{dest.password}")],
            )
            zk_d.start()
            zk_d.ensure_path(dest.password)
            logger.debug(
                "Connected to destination Zookeeper at %s:%s", dest.hostname, dest.port
            )
            if zk_d.exists(dest.path):
                if args.force:
                    zk_d.set(
                        dest.path,
                        json.dumps(cfg_src.get(), default=utils.json_serial),
                    )
                else:
                    print("Destination node already exists, use --force to overwrite")
            else:
                zk_d.create(
                    dest.path,
                    json.dumps(cfg_src.get(), default=utils.json_serial),
                    makepath=True,
                )
            zk_d.stop()
        else:
            if len(args.dest) > 5 and args.dest[-5:] == ".json":
                cfg_src.write().json(args.dest)
            else:
                cfg_src.write().cfg(args.dest)
    if args.source:
        src: urlparse.ParseResult = urlparse.urlparse(args.source)
        if src.scheme == "zookeeper":
            zk_s.stop()


if __name__ == "__main__":
    main()
