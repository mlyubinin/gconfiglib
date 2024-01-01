""" Root configuration node."""

import logging
import os
from typing import Callable, List, Optional, Type
from urllib import parse as urlparse

import gconfiglib.globals as glb
from gconfiglib import utils
from gconfiglib.config_node import ConfigNode
from gconfiglib.config_reader import ConfigReader
from gconfiglib.config_writer import ConfigWriter
from gconfiglib.enums import NodeType
from gconfiglib.template_node_base import TemplateNodeBase
from gconfiglib.template_node_fixed import TemplateNodeFixed

logger = logging.getLogger()


class ConfigRoot(ConfigNode):
    """Root configuration node.
    Differs from ConfigNode in that it includes methods for initializing
    and saving configuration from files or services."""

    def __init__(
        self,
        filename: Optional[str] = None,
        default_paths: Optional[List[str]] = None,
        default_env_path: Optional[str] = None,
        template_gen: Optional[Callable[[ConfigNode], Type[TemplateNodeBase]]] = None,
    ) -> None:
        """
        Initialization of ConfigRoot is done by loading configuration from file
        by selecting first viable candidate from candidate hierarchy.

        For loading from file, hierarchy (in order of preference):
        1. Explicit file name
        2. File name in environment variable
        3. List of file names in default paths
        File name can also be a zookeeper URI in the format zookeeper://user:password@host:port/path

        Configuration can also be immediately validated against a template, by passing template generator function.
        This will assign default values to any attributes missing in either explicit assignment or in configuration file

        :param filename: Explicit file name
        :param default_paths: List of file names
        :param default_env_path: Name of environment variable that stores the file name
        :param template_gen: Function that takes configuration as parameter and generates the validation template

        """

        # Form candidate list
        candidate_list: List[str] = []
        if filename:
            candidate_list.append(filename)
        if default_env_path and default_env_path in os.environ:
            candidate_list.append(os.environ[default_env_path])
        if default_paths and isinstance(default_paths, list):
            candidate_list.extend(default_paths)

        if len(candidate_list) > 0:
            # Read configuration from a file
            for fname in candidate_list:
                try:
                    candidate_uri = urlparse.urlparse(fname)
                    if candidate_uri.scheme == "zookeeper":
                        if not glb.zk_conn:
                            glb.zk_conn = utils.zk_connect(fname)
                        self._copy(ConfigReader().zk(candidate_uri.path))
                        if hasattr(self, "attributes") and len(self.attributes) > 0:
                            glb.zk_update = True

                            # Set data watch
                            @glb.zk_conn.DataWatch(self.zk_path)
                            def cfg_refresh(data, stat):
                                """
                                Hook to refresh configuration object when data in Zookeeper node changes
                                :param data: Not used, required by Zookeeper API
                                :param stat: Zookeeper statistics object, used to get version
                                """

                                if not glb.zk_update:
                                    glb.zk_update = True
                                    self._copy(ConfigReader().zk(self.zk_path))
                                    if template_gen:
                                        # Validate new configuration
                                        template = template_gen(self)
                                        if (
                                            isinstance(template, TemplateNodeFixed)
                                            and template.name == "root"
                                        ):
                                            self._copy(template.validate(self))
                                    logger.debug(
                                        "Refreshing configuration to version %s",
                                        stat.version,
                                    )
                                    glb.zk_update = False

                            glb.zk_update = False

                            # Set child watches
                            if self.node_type == NodeType.AN:
                                glb.zk_update = True

                                @glb.zk_conn.ChildrenWatch(self.zk_path)
                                def cfg_child_refresh(children):
                                    """
                                    Hook to refresh configuration object when data in Zookeeper child nodes changes
                                    :param children: Not used, required by Zookeeper API
                                    """

                                    if not glb.zk_update:
                                        glb.zk_update = True
                                        self._copy(ConfigReader().zk(self.zk_path))
                                        if template_gen:
                                            # Validate new configuration
                                            template = template_gen(self)
                                            self._copy(template.validate(self))
                                        logger.debug(
                                            "Refreshing configuration due to chile node changes",
                                        )
                                        glb.zk_update = False

                                glb.zk_update = False
                            glb.zk_update = False

                    elif len(fname) > 5 and fname[-5:] == ".json":
                        self._copy(ConfigReader().json(fname))
                        self.set_node_type(NodeType.CN)
                    else:
                        self._copy(ConfigReader().cfg(fname))
                        self.set_node_type(NodeType.CN)
                except:  # noqa: E722
                    continue

                if hasattr(self, "attributes") and len(self.attributes) > 0:
                    # Configuration has been read successfully
                    break

            if hasattr(self, "attributes") and len(self.attributes) == 0:
                raise IOError(
                    f"Could not read configuration file from any of the specified sources: {candidate_list}"
                )

        if template_gen:
            # Validate configration
            self.template_gen = template_gen
            template: Type[TemplateNodeBase] = template_gen(self)
            if isinstance(template, TemplateNodeFixed) and template.name == "root":
                self._copy(template.validate(self))
            else:
                logger.error("Invalid configuration template")

        if not hasattr(self, "attributes") or len(self.attributes) == 0:
            logger.critical("Could not initialize configuration")
            raise ValueError("Could not initialize configuration")

    @staticmethod
    def read() -> ConfigReader:
        """
        Generates a reader for this node
        :return: ConfigReader object
        """
        return ConfigReader()

    def write(self) -> ConfigWriter:
        """
        Generates a writer for this node
        :return: ConfigWriter object
        """
        return ConfigWriter(self)
