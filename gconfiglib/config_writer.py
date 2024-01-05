""" Config Writer class."""

import json
import logging
import os
from typing import Optional

from kazoo.client import KazooClient

from gconfiglib import utils
from gconfiglib.config_attribute import ConfigAttribute
from gconfiglib.config_node import ConfigNode
from gconfiglib.enums import NodeType

logger = logging.getLogger(__name__)


class ConfigWriter:
    """
    Configuration file writer
    """

    def __init__(self, cfg_obj: ConfigNode):
        self.cfg_obj = cfg_obj

    def _check_file(self, filename: str, force: bool) -> bool:
        """
        Internal method. Checks that the file exists
        :param filename: Filename to check
        :param force: Force file overwrite (True/False)
        :return: True if file does not exist, or if force is set to True. Raises IOError otherwise
        """
        if os.path.isfile(filename) and not force:
            logger.error("File %s already exists", filename)
            raise IOError(f"File {filename} already exists")
        return True

    def cfg(self, filename: str, force: bool = False) -> None:
        """
        Writer for plain text config files
        :param filename: Filename
        :param force: Force file overwrite (True/False)
        """
        # TODO: add support for writing root-level attributes
        logger.info("Writing config file %s", filename)
        try:
            self._check_file(filename, force)
        except IOError as e:
            logger.exception("Failed to open the file %s", filename, exc_info=True)
            raise IOError(f"Failed to open the file {filename}", e) from e
        with open(filename, mode="w", encoding="utf-8") as f:
            for node in self.cfg_obj.attributes.values():
                if isinstance(node, ConfigAttribute):
                    logger.error("cfg format does not support attributes at root level")
                    raise ValueError(
                        "cfg format does not support attributes at root level"
                    )
                f.write(f"\n[{node.name}]\n")
                for attribute in node.attributes.values():
                    if isinstance(attribute, ConfigNode):
                        logger.error(
                            "cfg format does not support multi-level hierarchy"
                        )
                        raise ValueError(
                            "cfg format does not support multi-level hierarchy"
                        )
                    f.write(f"{attribute.name} = {attribute.value}\n")
        logger.debug("Successfully saved configuration in %s", filename)

    def json(self, filename: str, force: bool = False) -> None:
        """
        Writer for json config files
        :param filename: Filename
        :param force: Force file overwrite (True/False)
        """
        logger.info("Writing config file %s", filename)
        try:
            self._check_file(filename, force)
        except IOError as e:
            logger.exception("Failed to open the file %s", filename, exc_info=True)
            raise IOError(f"Failed to open the file {filename}", e) from e
        with open(filename, mode="w", encoding="utf-8") as f:
            json.dump(
                self.cfg_obj.get(),
                f,
                ensure_ascii=True,
                indent=4,
                default=utils.json_serial,
                separators=(",", ": "),
            )
        logger.debug("Successfully saved configuration in %s", filename)

    def zk(self, path: Optional[str] = None, force: bool = False) -> None:
        """
        Writer for Zookeeper
        :param path: path to root node in Zookeeper
        :param force: Force file overwrite (True/False)
        """

        logger.info("Saving configuration to Zookeeper")
        root = self.cfg_obj.get_root()
        if not root.zk_conn:
            logger.error("No open Zookeeper connection")
            raise IOError("No open Zookeeper connection")
        if not root.zk_update:
            # Lock configuration from getting updated
            root.zk_update = True
            if path is None:
                path = self.cfg_obj.zk_path

            if self.cfg_obj.node_type == NodeType.C:
                logger.error(
                    "Write method called on Content node %s", self.cfg_obj.name
                )
                raise AttributeError(
                    f"write method called on Content node {self.cfg_obj.name}"
                )
            elif self.cfg_obj.node_type == NodeType.CN:
                content = json.dumps(
                    self.cfg_obj.get(), ensure_ascii=True, default=utils.json_serial
                )
            elif (
                self.cfg_obj.node_type == NodeType.AN
                and len(self.cfg_obj.list_attributes()) > 0
            ):
                content = json.dumps(
                    self.cfg_obj.get_attributes(),
                    ensure_ascii=True,
                    default=utils.json_serial,
                )
            else:
                content = json.dumps({})

            if root.zk_conn.exists(path):
                if force:
                    # need to make sure there are no "orphans" from previous version of the configuration
                    root.zk_update = False
                    root.zk_conn.delete(path, recursive=True)
                    root.zk_conn.create(path, content.encode(), makepath=True)
                    root.zk_update = True
                else:
                    logger.error(
                        "Failed to save configuration - path already exists and force attribute is not set"
                    )
                    raise IOError(
                        "Failed to save configuration - path already exists and force attribute is not set"
                    )
            else:
                root.zk_conn.create(path, content.encode(), makepath=True)
            if self.cfg_obj.node_type == NodeType.AN:
                for node_name in self.cfg_obj.list_nodes():
                    root.zk_update = False
                    ConfigWriter(self.cfg_obj._get_obj(node_name)).zk(
                        f"{path}/{node_name}", force=force
                    )
                    root.zk_update = True
            root.zk_update = False
        logger.debug("Successfully saved configuration to Zookeeper")
