""" Configuration Reader class."""

import json
import logging
import os
from collections import OrderedDict
from typing import List, Optional, Tuple

from kazoo.client import KazooClient

from gconfiglib import utils
from gconfiglib.config_node import ConfigNode
from gconfiglib.enums import NodeType

logger = logging.getLogger(__name__)


class ConfigReader:
    """
    Configuration file reader
    """

    @staticmethod
    def cfg(filename: str) -> Optional[ConfigNode]:
        """
        Reader for plain text config files
        :param filename: Filename
        :return: ConfigNode with file contents parsed into nodes and attributes
        """
        if check_file(filename):
            logger.info("Reading config file %s", filename)
            return ConfigNode("root", attributes=read_config(filename))
        return None

    @staticmethod
    def json(filename: str) -> Optional[ConfigNode]:
        """
        Reader for json config files
        :param filename: Filename
        :return: ConfigNode with file contents parsed into nodes and attributes
        """
        if check_file(filename):
            logger.info("Reading config file %s", filename)
            with open(filename, "r", encoding="utf-8") as f:
                return ConfigNode(
                    "root",
                    attributes=json.load(f, object_pairs_hook=utils.json_decoder),
                )
        return None

    @staticmethod
    def zk(
        connection: Optional[KazooClient], path: str, name: str = "root"
    ) -> Optional[ConfigNode]:
        """
        Reader for Zookeeper
        :param path: path to root node in Zookeeper
        :param name: name to give root node (defaults to 'root')
        :return: ConfigNode
        """

        if not connection:
            logger.error("No open Zookeeper connection")
            raise IOError("No open Zookeeper connection")
        if not connection.exists(path):
            logger.error("Path %s does not exist", path)
        else:
            children: Optional[List[str]] = connection.get_children(path)
            try:
                logger.debug("Reading node %s at path %s", name, path)
                node = ConfigNode(
                    name,
                    attributes=json.loads(
                        connection.get(path)[0], object_pairs_hook=utils.json_decoder
                    ),
                    node_type=NodeType.CN,
                )
            except ValueError as e:
                if str(e) == "No JSON object could be decoded" and len(children) > 0:
                    node = ConfigNode(name)
                else:
                    logger.exception(
                        "Unable to read the node at path %s", path, exc_info=True
                    )
                    raise
            node.zk_path = path
            if len(children) > 0:
                node.set_node_type(NodeType.AN)
                for child in children:
                    node.add(ConfigReader().zk(connection, f"{path}/{child}", child))
            return node
        return None


def check_file(filename: str) -> bool:
    """
    Internal method. Checks that the file exists
    :param filename: Filename to check
    :return: True is file exists and is readable
    """
    if os.path.isfile(filename) and os.access(filename, os.R_OK):
        return True
    logger.exception(
        "File %s does not exist or is not readable", filename, exc_info=True
    )
    raise IOError(f"File {filename} does not exist or is nor readable")


def read_config(
    file_name: str,
) -> OrderedDict[str, str | OrderedDict[str, str | List[str]]]:
    """
    Reads configuration from a file
    :param file_name: name of the configuration file
    :return: dictionary object with config key-value pairs
    :return: dictionary object with config key-value pairs
    """
    conf: OrderedDict[str, str | OrderedDict[str, str | List[str]]] = OrderedDict()

    cur_section: str = ""
    # Read the file
    if os.path.isfile(file_name) and os.access(file_name, os.R_OK):
        with open(file_name, "r", encoding="utf-8") as config_file:
            config_data: List[str] = config_file.readlines()
        # For every line:
        for line in config_data:
            config_key, config_value = parse_config_line(line)
            if config_key == 0:
                continue
            elif config_key == 1:
                cur_section = config_value
                conf[cur_section] = OrderedDict()
                continue

            # Assign to section or sectionless
            if cur_section == "":
                conf[config_key] = config_value
            else:
                conf[cur_section][config_key] = config_value
        if not conf:
            logger.exception("Empty configuration file %s", file_name, exc_info=True)
            raise IOError(f"Empty configuration file {file_name}")
        return conf
    logger.exception(
        "File %s does not exist or is not readable", file_name, exc_info=True
    )
    raise FileNotFoundError(f"File {file_name} does not exist or is not readable")


def parse_config_line(line: str) -> Tuple[str | int, str | List[str]]:
    """
    :param line: string with a single line from config file
    :return: config_key and config_value
        config_key: 0 - empty line or comment
                    1 - section (section name in line)
    """

    # Remove comments and strip whitespace
    line = line.split("#")[0]
    line = line.strip()
    if len(line) <= 2:
        # Line too short - not a configuration parameter
        return 0, line

    if line[0] == "[" and line[len(line) - 1] == "]":
        # Starting line of new section
        line = line[1 : len(line) - 1].strip()
        if len(line) > 0:
            return 1, line
        return 0, line

    # Regular configuration parameter
    pair: List[str] = line.split("=", maxsplit=1)
    # only the first '=' matters
    if len(pair) == 1:
        # no separator
        return 0, line
    config_key, config_value = pair
    config_key = config_key.strip()
    config_value = config_value.strip()
    if config_key == "" or config_value == "":
        return 0, line

    if (
        len(config_value) > 2
        and config_value[0] == "["
        and config_value[len(config_value) - 1] == "]"
    ):
        # Value is a list
        lst: List[str] = config_value[1 : len(config_value) - 1].split(",")
        for i, _ in enumerate(lst):
            lst[i] = lst[i].strip()
        config_value = lst

    return config_key, config_value
