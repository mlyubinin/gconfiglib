# Copyright (C) 2019 Michael Lyubinin
# Author: Michael Lyubinin
# Contact: michael@lyubinin.com

""" Gconfiglib enhanced configuration library. """

import argparse
import datetime as dt
import importlib
import json
import logging
import os
import sys
from ast import literal_eval
from collections import OrderedDict
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar
from urllib import parse as urlparse

from kazoo.client import KazooClient
from kazoo.security import make_digest_acl

from gconfiglib import utils

# Internal module variables. Values are assigned at runtime
# Root configuration node
_cfg_root: Optional["ConfigNode"] = None
# Zookeeper connection (if using zookeeper)
_zk_conn: Optional[KazooClient] = None
# Zookeeper update flag (used for managing update collisions in triggers)
_zk_update: bool = False


def get(path: Optional[str] = None) -> OrderedDict[str, Any] | Any:
    """
    Get configuration as OrderedDict starting at specific path relative to root
    :param path: path from root, str. If None, treat as '/'
    :return: OrderedDict if path ends in a node, otherwise value of an attribute at path
    :raises: AttributeError if configuration has not been initialized
    """
    global _cfg_root
    return _cfg_root.get(path)


def set(path: str, value: Any) -> None:
    """
    Assign new value to existing node/attribute or add a new node/attribute at path
    :param path: path from root, str.
    :param value: Can be ConfigNode, ConfigAttribute, dictionary, or a list any of the above to assign at node path, or
                    any other value to set an attribute value
    :raises: AttributeError if configuration has not been initialized
    """
    global _cfg_root
    _cfg_root.set(path, value)


# TODO: move Config classes out and import them
def root() -> Optional["ConfigNode"]:
    """
    Return root configuration object or None if it's not initialized
    """
    global _cfg_root
    return _cfg_root


# TODO: move Template classes out and import them
def init(
    filename: Optional[str] = None,
    default_paths: Optional[List[str]] = None,
    default_env_path: Optional[str] = None,
    template_gen: Optional[Callable[["ConfigNode"], "Type[TemplateNodeBase]"]] = None,
) -> None:
    """
    Initialize Config object, selecting first viable candidate from candidate hierarchy.
    Hierarchy (in order of preference):
    1. Explicit file name
    2. File name in environment variable
    3. List of file names in default paths
    File name can also be a zookeeper URI in the format zookeeper://user:password@host:port/path
    :param filename: Explicit file name
    :param default_paths: List of file names
    :param default_env_path: Name of environment variable that stores the file name
    :param template_gen: Function that takes configuration as parameter and generates the validation template
    """

    global _cfg_root
    global _zk_conn
    global _zk_update

    logger = logging.getLogger("gconfiglib")
    # Form candidate list
    candidate_list: List[str] = []
    if filename:
        candidate_list.append(filename)
    if default_env_path and default_env_path in os.environ:
        candidate_list.append(os.environ[default_env_path])
    if default_paths and isinstance(default_paths, list):
        candidate_list.extend(default_paths)

    # Attempt to read candidates, fail if explicit file is invalid or if no configuration found
    for fname in candidate_list:
        try:
            logger.debug("Trying to read configuration from %s", fname)
            candidate_uri: urlparse.ParseResult = urlparse.urlparse(fname)
            if candidate_uri.scheme == "zookeeper":
                if not _zk_conn:
                    _zk_conn = utils.zk_connect(fname)
                _cfg_root = ConfigNode.read().zk(candidate_uri.path)
                if _cfg_root is not None:
                    _zk_update = True
                    # Set data watch

                    @_zk_conn.DataWatch(_cfg_root.zk_path)
                    def cfg_refresh(data, stat):
                        """
                        Hook to refresh configuration object when data in Zookeeper node changes
                        :param data: Not used, required by Zookeeper API
                        :param stat: Zookeeper statistics object, used to get version
                        """
                        global _cfg_root
                        global _zk_update
                        template_gen: Optional[
                            Callable[["ConfigNode"], "Type[TemplateNodeBase]"]
                        ] = None
                        if not _zk_update:
                            _zk_update = True
                            if _cfg_root:
                                # if refreshing existing node, get the configuration template
                                template_gen = _cfg_root.template_gen
                            _cfg_root = ConfigNode.read().zk(_cfg_root.zk_path)
                            if template_gen:
                                # Validate new configuration
                                template = template_gen(_cfg_root)
                                if (
                                    isinstance(template, TemplateNodeFixed)
                                    and template.name == "root"
                                ):
                                    _cfg_root = template.validate(_cfg_root)
                            logger.debug(
                                "Refreshing configuration to version %s",
                                stat.version,
                            )
                            _zk_update = False

                    _zk_update = False
                    # Set child watches
                    if _cfg_root.node_type == "AN":
                        _zk_update = True

                        @_zk_conn.ChildrenWatch(_cfg_root.zk_path)
                        def cfg_child_refresh(children):
                            """
                            Hook to refresh configuration object when data in Zookeeper child nodes changes
                            :param children: Not used, required by Zookeeper API
                            """
                            global _cfg_root
                            global _zk_update
                            if not _zk_update:
                                _zk_update = True
                                if _cfg_root:
                                    # if refreshing existing node, get the configuration template
                                    template_gen = _cfg_root.template_gen
                                else:
                                    template_gen = None
                                _cfg_root = ConfigNode.read().zk(_cfg_root.zk_path)
                                if template_gen:
                                    # Validate new configuration
                                    template = template_gen(_cfg_root)
                                    if (
                                        isinstance(template, TemplateNodeFixed)
                                        and template.name == "root"
                                    ):
                                        _cfg_root = template.validate(_cfg_root)
                                logger.debug(
                                    "Refreshing configuration due to chile node changes",
                                )
                                _zk_update = False

                        _zk_update = False
                    _zk_update = False
            elif len(fname) > 5 and fname[-5:] == ".json":
                # Try to read JSON format file
                _cfg_root = ConfigNode.read().json(fname)
            else:
                # Try to read .ini format file
                _cfg_root = ConfigNode.read().cfg(fname)
        except Exception as e:
            if filename and filename == fname:
                raise FileNotFoundError(
                    f"Could not read configuration file {fname}", e
                ) from e
            else:
                continue
        logger.debug("Read configuration from %s", fname)
        if _cfg_root is not None:
            break

    # Validate configration
    if template_gen:
        _cfg_root.template_gen = template_gen
        template = template_gen(_cfg_root)
        if isinstance(template, TemplateNodeFixed) and template.name == "root":
            _cfg_root = template.validate(_cfg_root)
        else:
            logger.error("Invalid configuration template")
    if _cfg_root is None:
        logger.critical("Could not initialize configuration")
        raise ValueError("Could not initialize configuration")


class TemplateBase:
    """
    Common elements for all template objects
    """

    def __init__(
        self,
        optional: bool = True,
        validator: Optional[Callable[[Any], bool]] = None,
        description: Optional[str] = None,
    ) -> None:
        self.optional = optional
        self.validator = validator
        self.description = description

    def sample(self, format: str = "JSON") -> str:
        """Generate sample configuration

        Args:
            format (str, optional): JSON or TEXT. Defaults to "JSON".

        Returns:
            str: Sample configuration as string
        """
        return ""

    def validate(
        self, value: Optional["ConfigNode | ConfigAttribute"]
    ) -> Optional["ConfigNode | ConfigAttribute"]:
        """
        Validate an attribute or a node
        :param value: Value to be validated
        :return: validated value (possibly changed from original)
        """
        return value


class TemplateAttributeBase(TemplateBase):
    """
    Base attribute template
    """

    def __init__(
        self,
        optional: bool = True,
        value_type: type = str,
        validator: Optional[Callable[[Any], bool]] = None,
        default_value: Optional[Any] = None,
        description: Optional[str] = None,
    ) -> None:
        """
        :param optional: Is this attribute optional (True) or mandatory (False)
        :param value_type: Value type (int, str, etc.)
        :param validator: Validator function. Should take value as argument and return True or False
                            Raises ValueError on any validation failure
        :param default_value: Value to assign if missing from configuration object
        :param description: Attribute description (used when generating sample configuration files)
        """
        self.value_type = value_type
        self.default_value = default_value
        super().__init__(optional, validator, description)

    def validate(
        self, value: Optional["ConfigAttribute"], name: str
    ) -> Optional["ConfigAttribute"]:
        """
        Validate an attribute
        :param value: Value to be validated
        :param name: Name of the attribute to be validated
        :return: validated value (possibly changed from original), or raises ValueError on failure to validate
        """
        if value is None:
            value = ConfigAttribute(name, self.default_value)
        elif value.value is not None and not isinstance(value.value, self.value_type):
            try:
                value.value = self.value_type(value.value)
            except ValueError as e:
                if self.value_type == dt.date and isinstance(value.value, dt.datetime):
                    value.value = value.value.date()
                else:
                    if value.value is not None and value.value != "":
                        raise ValueError(
                            f"Expecting {value.get_path()} to be of type {self.value_type}"
                        ) from e

        if self.validator is not None:
            if not self.optional or value.value is not None:
                try:
                    valid = self.validator(value.value)
                    problem = ""
                except ValueError:
                    valid = False
                    problem = sys.exc_info()[0]
                if not valid:
                    message = f"Parameter {value.get_path()} failed validation for value {value.value}"
                    if problem != "":
                        message += f": {problem}"
                    raise ValueError(message)
        if not self.optional and value.value is None:
            # mandatory attribute with no value and no default
            raise ValueError(
                f"Mandatory parameter {value.get_path()} has not been set, and has no default value"
            )

        if value.value is None:
            return None
        return value

    def sample(self, format: str = "JSON") -> str:
        """
        Generate a line for sample configuration file
        :param format: JSON or TEXT
        :return: string
        """
        # TODO change format parameter to enum
        name: str = getattr(self, "name", "Attribute")
        value = self.default_value if self.default_value is not None else ""
        description: str = self.description if self.description is not None else ""
        if format == "JSON":
            return f'"{name}" : {json.dumps(value, ensure_ascii=True, default=utils.json_serial)}'
        if format == "TEXT":
            return f"#\n# {description}\n# {name} = {value}\n"
        return ""


class TemplateAttributeFixed(TemplateAttributeBase):
    """
    Configuration attribute template class
    For an attribute with a fixed name
    """

    def __init__(
        self,
        name: str,
        optional: bool = True,
        value_type: type = str,
        validator: Optional[Callable[[Any], bool]] = None,
        default_value: Any = None,
        description: Optional[str] = None,
    ) -> None:
        """
        :param name: Attribute name
        :param optional: Is this attribute optional (True) or mandatory (False)
        :param value_type: Value type (int, str, etc.)
        :param validator: Validator function. Should take value as argument and return new, possibly changed value
                            Raises ValueError on any validation failure
        :param default_value: Value to assign if missing from configuration object
        :param description: Attribute description (used when generating sample configuration files)
        """
        self.name = name
        super().__init__(optional, value_type, validator, default_value, description)

    def validate(
        self, value: Optional["ConfigAttribute"]
    ) -> Optional["ConfigAttribute"]:
        """
        Validate an attribute
        :param value: Value to be validated
        :return: validated value (possibly changed from original), or raises ValueError on failure to validate
        """
        return super().validate(value, self.name)


class TemplateAttributeVariable(TemplateAttributeBase):
    """
    Variable configuration attribute template class
    For an attribute with a name not known until runtime
    """

    def __init__(
        self,
        value_type: type = str,
        validator: Optional[Callable[[Any], bool]] = None,
        description: Optional[str] = None,
    ) -> None:
        """
        :param value_type: Value type (int, str, etc.)
        :param validator: Validator function. Should take value as argument and return new, possibly changed value
                            Raises ValueError on any validation failure
        :param description: Attribute description (used when generating sample configuration files)
        """
        super().__init__(True, value_type, validator, description=description)


class TemplateNodeBase(TemplateBase):
    """
    Node template base class
    """

    def __init__(
        self,
        name: str,
        optional: bool = True,
        validator: Optional[Callable[[OrderedDict[str, Any]], bool]] = None,
        description: Optional[str] = None,
        node_type: Optional[str] = None,
    ) -> None:
        """
        :param name: Node name
        :param optional: Is this node optional (True) or mandatory (False)
        :param validator: Validator function. Should take value as argument and return new, possibly changed value
                            Raises ValueError on any validation failure
        :param description: Node description (used when generating sample configuration files)
        :param node_type: Node type: C (content), CN (content node), AN (abstract node)
        """
        self.name = name
        self.attributes: OrderedDict[str, TemplateBase] = OrderedDict()
        if node_type in ["C", "CN", "AN"]:
            self.node_type = node_type
        else:
            self.node_type = None
        super().__init__(optional, validator, description)

    def validate(self, node: Optional["ConfigNode"]) -> Optional["ConfigNode"]:
        """
        Validate a node
        :param node: Node to be validated
        :return: validated node (possibly changed from original), or raises ValueError on failure to validate
        """
        if node is None:
            # Empty node - try creating a node if it's mandatory, otherwise return None
            if self.optional:
                return None
            node = ConfigNode(self.name)
        elif not isinstance(node, ConfigNode):
            raise ValueError(
                "Configuration object passed for validation to template %s is not a ConfigNode"
                % self.name
            )
        elif len(self.attributes) == 0:
            raise ValueError(f"Template for node {self.name} has no attributes")
        # TODO self.validator needs to be run after attributes' validator functions have been run
        if self.validator is not None:
            try:
                valid = self.validator(node.get())
                problem = ""
            except ValueError:
                valid = False
                problem = sys.exc_info()[0]
                # TODO replace this with info from traceback
            if not valid:
                message = f"Node {node.get_path()} failed validation"
                if problem != "":
                    message += f": {problem}"
                raise ValueError(message)
        if self.node_type and self.node_type != node.node_type:
            node.set_node_type(self.node_type)
        return node

    def sample(self, format: str = "JSON") -> str:
        """
        Generate a line for sample configuration file
        :param format: JSON or TEXT
        :return: string
        """
        description: str = self.description if self.description else ""
        if format == "JSON":
            if self.name == "root":
                result: str = "{"
            else:
                result = '"%s" : {' % self.name
            for attribute in self.attributes.values():
                result += attribute.sample(format) + ", "
            result = result[:-2] + "}"
        elif format == "TEXT":
            if self.name == "root":
                result = ""
            else:
                result = f"# {description}\n# [{self.name}]\n"
            for attribute in self.attributes.values():
                if self.name != "root" and isinstance(attribute, TemplateNodeBase):
                    raise ValueError(
                        "Text format configuration files are not supported for multi-level node hierarchy"
                    )
                result += attribute.sample(format)
        else:
            raise ValueError("Unsupported sample format")
        return result


class TemplateNodeFixed(TemplateNodeBase):
    """
    Configuration Node template class
    For a node with a fixed name
    """

    def add(self, attr: TemplateAttributeFixed | TemplateNodeBase) -> None:
        """
        Add child nodes/attribute templates
        :param attr: Any TemplateBase descendant object
        """
        if isinstance(attr, TemplateBase):
            if attr.name in self.attributes.keys():
                raise ValueError(
                    f"Attribute or node {attr.name} can only be added to node {self.name} once"
                )
            self.attributes[attr.name] = attr
        else:
            raise ValueError(
                f"Attempt to add invalid attribute type to {self.name} template"
            )

    def validate(self, node: Optional["ConfigNode"]) -> Optional["ConfigNode"]:
        """
        Validate a node
        :param node: Node to be validated
        :return: validated node (possibly changed from original), or raises ValueError on failure to validate
        """
        node = super().validate(node)
        # If None, pass it back without further checks (missing optional node was not created)
        if node is None:
            return None
        for attr_t_name, attr_t in self.attributes.items():
            if isinstance(attr_t, TemplateNodeSet):
                # For Node Set, need to pass in the full parent level object
                node = attr_t.validate(node)
            else:
                # For any other node or attribute, just pass the node/attribute itself
                # For attributes we need to make sure they are not None first
                if (
                    isinstance(attr_t, TemplateAttributeFixed)
                    and attr_t_name not in node.list_attributes()
                ):
                    test_attr = ConfigAttribute(
                        attr_t_name, value=attr_t.default_value, parent=node
                    )
                else:
                    test_attr = None

                new_value: "ConfigNode | ConfigAttribute" = attr_t.validate(
                    node._get_obj(attr_t_name)
                    if attr_t_name in [x.name for x in node.attributes.values()]
                    else test_attr
                )

                if (
                    isinstance(new_value, ConfigAttribute)
                    and new_value.value is not None
                ):
                    node.add(new_value)
                elif (
                    isinstance(new_value, ConfigNode) and len(new_value.attributes) > 0
                ):
                    node.add(new_value)

        if not self.optional and len(node.attributes) == 0:
            raise ValueError(
                f"Mandatory node {node.get_path()} is missing, with no defaults set"
            )
        if len(node.attributes) == 0:
            return None
        return node


class TemplateNodeVariableAttr(TemplateNodeBase):
    """
    Configuration Node with a variable list of attributes
    """

    def __init__(
        self,
        name: str,
        attr: TemplateAttributeVariable,
        optional: bool = True,
        validator: Optional[Callable[[OrderedDict[str, Any]], bool]] = None,
        description: Optional[str] = None,
        node_type: Optional[str] = None,
    ) -> None:
        """
        :param name: Node name
        :param attr: Attribute template. All attributes in VariableAttr node must be of the same type
        :param optional: Is this node optional (True) or mandatory (False)
        :param validator: Validator function. Should take value as argument and return new, possibly changed value
                            Raises ValueError on any validation failure
        :param description: Node description (used when generating sample configuration files)
        :param node_type: Node type: C (content), CN (content node), AN (abstract node)
        """
        super().__init__(name, optional, validator, description, node_type)
        self.attributes: OrderedDict[str, TemplateAttributeVariable] = OrderedDict()
        if not isinstance(attr, TemplateAttributeVariable):
            raise ValueError(
                f"Attempt to add invalid attribute type to {self.name} template. This node can contain only one TemplateAttributeVariable attribute template and nothing else"
            )
        self.attributes["variable_attribute"] = attr

    def validate(self, node: Optional["ConfigNode"]) -> Optional["ConfigNode"]:
        """
        Validate a node
        :param node: Node to be validated
        :return: validated node (possibly changed from original), or raises ValueError on failure to validate
        """
        node = super().validate(node)
        # If None, pass it back without further checks (missing optional node was not created)
        if node is None:
            return None
        if len(node.attributes) == 0 and not self.optional:
            raise ValueError(f"Node {node.get_path()} cannot be empty")
        for attr_name, attr_value in node.attributes.items():
            new_value = self.attributes["variable_attribute"].validate(
                attr_value, attr_name
            )
            if isinstance(new_value, ConfigAttribute) and new_value.value is not None:
                node.add(new_value)
            elif isinstance(new_value, ConfigNode) and len(new_value.attributes) > 0:
                node.add(new_value)

        if len(node.attributes) == 0:
            return None
        return node


class TemplateNodeSet(TemplateNodeBase):
    """
    Template class for a set of either fixed or variable attribute nodes
    """

    def __init__(self, name: str, node: TemplateNodeBase, names_lst: List[str]) -> None:
        """
        :param name: Nodeset name
        :param node: Node template. All nodes in NodeSet node must be of the same type
        :param names_lst: List of names of nodes that should be in the node set
        """
        super().__init__(name)
        self.attributes["node"] = node
        self.names_lst = names_lst
        if not isinstance(node, TemplateNodeBase):
            raise ValueError(
                "Node Set template can only be initialized with a valid node template object"
            )
        elif not isinstance(names_lst, list) or len(names_lst) == 0:
            raise ValueError(
                "Node Set template can only be initialized with a non-empty list of node names"
            )

    def validate(self, node: Optional["ConfigNode"]) -> Optional["ConfigNode"]:
        """
        Validate a node
        :param node: Node to be validated
        :return: validated node (possibly changed from original), or raises ValueError on failure to validate
        """
        logger = logging.getLogger("gconfiglib")

        node = super().validate(node)
        # If None, pass it back without further checks (missing optional node was not created)
        if node is None:
            return None
        for name in self.names_lst:
            if name not in node.attributes.keys():
                if not self.attributes["node"].optional:
                    node.add(ConfigNode(name))
                    logger.debug(
                        "Mandatory node %s is missing in %s", name, node.get_path()
                    )
                else:
                    logger.debug(
                        "Optional node %s is missing in %s",
                        name,
                        node.get_path(),
                    )
                    continue
            new_value = self.attributes["node"].validate(node._get_obj(name))
            if isinstance(new_value, ConfigAttribute) and new_value.value is not None:
                node.add(new_value)
            elif isinstance(new_value, ConfigNode) and len(new_value.attributes) > 0:
                node.add(new_value)
        if len(node.attributes) == 0:
            return None
        return node

    def sample(self, format: str = "JSON") -> str:
        """
        Generate a line for sample configuration file
        :param format: JSON or TEXT
        :return: string
        """
        description: str = (
            self.attributes["node"].description
            if self.attributes["node"].description
            else ""
        )
        result: str
        for node_name in self.names_lst:
            if format == "JSON":
                result = '"%s" : {' % node_name
                for attribute in self.attributes["node"].attributes.values():
                    result += attribute.sample(format) + ", "
                result = result[:-2] + "}, "
            elif format == "TEXT":
                result = f"# {description}\n# [{node_name}]\n"
                for attribute in self.attributes["node"].attributes.values():
                    if isinstance(attribute, TemplateNodeBase):
                        raise ValueError(
                            "Text format configuration files are not supported for multi-level node hierarchy"
                        )
                    result += attribute.sample(format)
            else:
                raise ValueError("Unsupported sample format")
        if format == "JSON":
            result = result[:-2]
        return result


class ConfigReader:
    """
    Configuration file reader
    """

    def _check_file(self, filename: str) -> bool:
        """
        Internal method. Checks that the file exists
        :param filename: Filename to check
        :return: True is file exists and is readable
        """
        if os.path.isfile(filename) and os.access(filename, os.R_OK):
            return True
        raise IOError(f"File {filename} does not exist or is nor readable")

    def cfg(self, filename: str) -> Optional["ConfigNode"]:
        """
        Reader for plain text config files
        :param filename: Filename
        :return: ConfigNode with file contents parsed into nodes and attributes
        """
        if self._check_file(filename):
            return ConfigNode("root", attributes=read_config(filename), node_type="CN")
        return None

    def json(self, filename: str) -> Optional["ConfigNode"]:
        """
        Reader for json config files
        :param filename: Filename
        :return: ConfigNode with file contents parsed into nodes and attributes
        """
        if self._check_file(filename):
            with open(filename, "r", encoding="utf-8") as f:
                return ConfigNode(
                    "root",
                    attributes=json.load(f, object_pairs_hook=utils.json_decoder),
                    node_type="CN",
                )
        return None

    def zk(self, path: str, name: str = "root") -> Optional["ConfigNode"]:
        """
        Reader for Zookeeper
        :param path: path to root node in Zookeeper
        :param name: name to give root node (defaults to 'root')
        :return: ConfigNode
        """
        global _zk_conn

        logger = logging.getLogger("gconfiglib")

        if not _zk_conn:
            raise IOError("No open Zookeeper connection")
        if not _zk_conn.exists(path):
            logger.error("Path %s does not exist", path)
        else:
            children: Optional[List[str]] = _zk_conn.get_children(path)
            try:
                node = ConfigNode(
                    name,
                    attributes=json.loads(
                        _zk_conn.get(path)[0], object_pairs_hook=utils.json_decoder
                    ),
                    node_type="CN",
                )
            except ValueError as e:
                if str(e) == "No JSON object could be decoded" and len(children) > 0:
                    node = ConfigNode(name)
                else:
                    raise
            node.zk_path = path
            if len(children) > 0:
                node.set_node_type("AN")
                for child in children:
                    node.add(ConfigNode.read().zk(f"{path}/{child}", child))
            return node
        return None


class ConfigWriter:
    """
    Configuration file writer
    """

    def __init__(self, cfg_obj: "ConfigNode"):
        self.cfg_obj = cfg_obj

    def _check_file(self, filename: str, force: bool) -> bool:
        """
        Internal method. Checks that the file exists
        :param filename: Filename to check
        :param force: Force file overwrite (True/False)
        :return: True if file does not exist, or if force is set to True. Raises IOError otherwise
        """
        if os.path.isfile(filename) and not force:
            raise IOError(f"File {filename} already exists")
        return True

    def cfg(self, filename: str, force: bool = False) -> None:
        """
        Writer for plain text config files
        :param filename: Filename
        :param force: Force file overwrite (True/False)
        """
        # TODO: add support for writing root-level attributes
        try:
            self._check_file(filename, force)
        except IOError as e:
            raise IOError(f"Failed to open the file {filename}", e) from e
        with open(filename, mode="w", encoding="utf-8") as f:
            for node in self.cfg_obj.attributes.values():
                if isinstance(node, ConfigAttribute):
                    raise ValueError(
                        "cfg format does not support attributes at root level"
                    )
                f.write(f"\n[{node.name}]\n")
                for attribute in node.attributes.values():
                    if isinstance(attribute, ConfigNode):
                        raise ValueError(
                            "cfg format does not support multi-level hierarchy"
                        )
                    f.write(f"{attribute.name} = {attribute.value}\n")

    def json(self, filename: str, force: bool = False) -> None:
        """
        Writer for json config files
        :param filename: Filename
        :param force: Force file overwrite (True/False)
        """
        try:
            self._check_file(filename, force)
        except IOError as e:
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

    def zk(self, path: Optional[str] = None, force: bool = False) -> None:
        """
        Writer for Zookeeper
        :param path: path to root node in Zookeeper
        :param force: Force file overwrite (True/False)
        """
        global _zk_conn
        global _zk_update
        if not _zk_conn:
            raise IOError("No open Zookeeper connection")
        if not _zk_update:
            # Lock configuration from getting updated
            _zk_update = True
            if path is None:
                path = self.cfg_obj.zk_path

            if self.cfg_obj.node_type == "C":
                raise AttributeError(
                    f"write method called on Content node {self.cfg_obj.name}"
                )
            elif self.cfg_obj.node_type == "CN":
                content = json.dumps(
                    self.cfg_obj.get(), ensure_ascii=True, default=utils.json_serial
                )
            elif (
                self.cfg_obj.node_type == "AN"
                and len(self.cfg_obj.list_attributes()) > 0
            ):
                content = json.dumps(
                    self.cfg_obj.get_attributes(),
                    ensure_ascii=True,
                    default=utils.json_serial,
                )
            else:
                content = json.dumps({})

            if _zk_conn.exists(path):
                if force:
                    # need to make sure there are no "orphans" from previous version of the configuration
                    _zk_update = False
                    _zk_conn.delete(path, recursive=True)
                    _zk_conn.create(path, content.encode(), makepath=True)
                    _zk_update = True
                else:
                    raise IOError(
                        "Failed to save configuration - path already exists and force attribute is not set"
                    )
            else:
                _zk_conn.create(path, content.encode(), makepath=True)
            if self.cfg_obj.node_type == "AN":
                for node_name in self.cfg_obj.list_nodes():
                    _zk_update = False
                    self.cfg_obj._get_obj(node_name).write().zk(
                        f"{path}/{node_name}", force=force
                    )
                    _zk_update = True
            _zk_update = False


class ConfigNode:
    """
    Configuration node class
    """

    # TODO: Change node_type to enum

    def __init__(
        self,
        name: str,
        parent: Optional["ConfigNode"] = None,
        attributes: Optional[
            "ConfigNode | ConfigAttribute"
            | Dict[Any, Any]
            | List["ConfigNode | ConfigAttribute" | Tuple[str, Any]]
        ] = None,
        node_type: str = "C",
        filename: Optional[str] = None,
        default_paths: Optional[List[str]] = None,
        default_env_path: Optional[str] = None,
        template_gen: Optional[
            Callable[["ConfigNode"], "Type[TemplateNodeBase]"]
        ] = None,
    ) -> None:
        """
        Initialization of ConfigNode can include:
        1. Loading configuration from file by selecting first viable candidate from candidate hierarchy.
        2. Explicit assignment of attributes by passing another ConfigNode, ConfigAttribute, a dictionary, or a list
        of any of the above
        3. Delayed assignment, which would create an empty node

        For loading from file, hierarchy (in order of preference):
        1. Explicit file name
        2. File name in environment variable
        3. List of file names in default paths
        File name can also be a zookeeper URI in the format zookeeper://user:password@host:port/path

        Node can also be immediately validated against a template, by passing template generator function.
        This will assign default values to any attributes missing in either explicit assignment or in configuration file

        :param name: Node name
        :param parent: Node's parent
        :param attributes: Node content
        :param node_type: Node type: C (content), CN (content node), AN (abstract node)
        :param filename: Explicit file name
        :param default_paths: List of file names
        :param default_env_path: Name of environment variable that stores the file name
        :param template_gen: Function that takes configuration as parameter and generates the validation template

        """
        self.name: str = name
        self.parent: Optional["ConfigNode"] = parent
        self.node_type: str = node_type
        self.zk_path: Optional[str] = None
        self.template_gen: Optional[
            Callable[["ConfigNode"], "Type[TemplateNodeBase]"]
        ] = None

        logger = logging.getLogger("gconfiglib")

        self.depth: int = 1
        if parent:
            self.depth = parent.depth + 1

        self.attributes: OrderedDict[
            str, "ConfigNode | ConfigAttribute"
        ] = OrderedDict()

        # Form candidate list
        candidate_list: List[str] = []
        if filename:
            candidate_list.append(filename)
        if default_env_path and default_env_path in os.environ:
            candidate_list.append(os.environ[default_env_path])
        if default_paths and isinstance(default_paths, list):
            candidate_list.extend(default_paths)

        if attributes:
            # Explicitly set attributes
            self.add(attributes)
        elif len(candidate_list) > 0:
            # Read configuration from a file
            for fname in candidate_list:
                try:
                    logger.debug("Trying to read configuration from %s", fname)
                    candidate_uri = urlparse.urlparse(fname)
                    if candidate_uri.scheme == "zookeeper":
                        if not ("_zk_conn" in locals() or "_zk_conn" in globals()):
                            _zk_conn = utils.zk_connect(fname)
                        elif not _zk_conn:
                            _zk_conn = utils.zk_connect(fname)
                        self._copy(ConfigNode.read().zk(candidate_uri.path))
                    elif len(fname) > 5 and fname[-5:] == ".json":
                        self._copy(ConfigNode.read().json(fname))
                    else:
                        self._copy(ConfigNode.read().cfg(fname))
                except:  # noqa: E722
                    continue

                if len(self.attributes) > 0:
                    # Configuration has been read successfully
                    break

            if len(self.attributes) == 0:
                raise IOError(
                    f"Could not read configuration file from any of the specified sources: {candidate_list}"
                )

        if template_gen:
            # Validate configration
            self.template_gen = template_gen
            template: Type[TemplateNodeBase] = template_gen(self)
            if isinstance(template, TemplateNodeFixed) and template.name == "root":
                self._copy(template.validate(self))

    def add(
        self,
        attributes: "ConfigNode | ConfigAttribute"
        | Dict[Any, Any]
        | List["ConfigNode | ConfigAttribute" | Tuple[str, Any]],
    ) -> None:
        """
        Add content to a node
        :param attributes: Can be ConfigNode, ConfigAttribute, a dictionary, or a list of any of the above
        """
        if isinstance(attributes, (ConfigNode, ConfigAttribute)):
            # A single ConfigNode or ConfigAttribute
            self.attributes[attributes.name] = attributes
            attributes._set_parent(self)
        elif isinstance(attributes, list):
            # List of items
            for attribute in attributes:
                if isinstance(attribute, (ConfigNode, ConfigAttribute)):
                    # A ConfigNode or ConfigAttribute as a list element
                    self.attributes[attribute.name] = attribute
                elif isinstance(attribute, tuple) and len(attribute) == 2:
                    # A tuple will result either in the node or an attribute
                    if isinstance(attribute[1], dict) or isinstance(attribute[1], list):
                        # (name, dictionary) or (name, list) - create a node
                        self.attributes[attribute[0]] = ConfigNode(
                            attribute[0], parent=self, attributes=attribute[1]
                        )
                    else:
                        # (name, value) - create an attribute
                        self.attributes[attribute[0]] = ConfigAttribute(
                            attribute[0], attribute[1], parent=self
                        )
                else:
                    raise ValueError(
                        "ConfigNode.add only accepts single ConfigNode, ConfigAttribute,"
                        "a list of ConfigNodes and/or ConfigAttributes",
                        "or a list of tuples that can be used to create nodes and/or attributes.",
                    )
        elif isinstance(attributes, dict):
            # Dictionary of items - dictionary elements will generate a node, other elements - an attribute
            for a_key, a_value in attributes.items():
                if isinstance(a_value, dict):
                    # for a dic element, create a node
                    self.attributes[a_key] = ConfigNode(
                        a_key, parent=self, attributes=a_value
                    )
                else:
                    # for any other element, create an attribute
                    self.attributes[a_key] = ConfigAttribute(
                        a_key, a_value, parent=self
                    )
        else:
            raise ValueError(
                "ConfigNode.add only accepts single ConfigNode, ConfigAttribute,"
                "or a list of ConfigNodes and/or ConfigAttributes"
            )

    def delete(self, path: str) -> None:
        """
        Delete content
        :param path: Path - everything at and below this path will be deleted
        """
        nodes = [x for x in path.split("/") if x != ""]
        if len(nodes) == 0:
            raise ValueError("No path to delete specified")
        if len(nodes) == 1:
            # the path corresponds to this node
            self.attributes.pop(nodes[0], None)
        else:
            # the path points to a node under this one
            # adjust path and call delete method of that node
            new_path = "/".join(nodes[1:])
            self.attributes[nodes[0]].delete(new_path)

    def _set_parent(self, parent_node: "ConfigNode") -> None:
        """
        Set node's parent
        :param parent_node: parent ConfigNode
        """
        self.parent = parent_node
        self.depth = parent_node.depth + 1
        for child in self.attributes.values():
            # recalculate depth for child nodes
            if isinstance(child, ConfigNode):
                child._set_parent(self)

    def _to_dict(self) -> OrderedDict[str, Any]:
        """
        Convert to dictionary
        :return: Node's content converted to OrderedDict
        """
        result: OrderedDict[str, Any] = OrderedDict()
        for attribute_name, attribute_value in self.attributes.items():
            result[attribute_name] = attribute_value._to_dict()
        return result

    def _get_obj(
        self, path: Optional[str] = None
    ) -> Optional["ConfigNode | ConfigAttribute"]:
        """
        Internal method. Retrieve object at path
        :param path: Path
        :return: ConfigNode or ConfigAttribute at specified path or None
        """
        if not path:
            # no path means this is the node to return
            return self
        # split path into components
        nodes: List[str] = [x for x in path.split("/") if x != ""]
        if len(nodes) == 0:
            # path that splits into an empty list means this is the node to return
            return self
        next_obj = nodes[0]
        new_path = "/".join(nodes[1:]) if len(nodes) > 1 else None
        if next_obj not in self.attributes.keys():
            # next level in the path does not exist in this node
            return None
        # go one level down the path recursively
        return self.attributes[next_obj]._get_obj(new_path)

    def get(self, path: Optional[str] = None) -> Optional[OrderedDict[str, Any] | Any]:
        """
        Retrieve object at path as OrderedDict
        :param path: Path
        :return: OrderedDict, attribute value or None
        """
        obj: Optional["ConfigNode | ConfigAttribute"] = self._get_obj(path)
        if obj is None:
            return None
        return obj._to_dict()

    def get_attributes(self, path: Optional[str] = None) -> OrderedDict[str, Any]:
        """
        Retrieve attributes of a node at path
        :param path: Path to a node
        :return: OrderedDict with 'attribute': 'value' pairs
        """
        if path:
            result: OrderedDict[str, Any] = self._get_obj(path).get_attributes()
        else:
            result = OrderedDict()
            for attribute_name, attribute_value in self.attributes.items():
                if isinstance(attribute_value, ConfigAttribute):
                    result[attribute_name] = attribute_value._to_dict()
        return result

    def set(self, path: str, value: Any) -> None:
        """
        Add or update content
        :param path: Path to node or attribute to update
        :param value: Value to assign
        """
        nodes: List[str] = [x for x in path.split("/") if x != ""]
        if len(nodes) == 0:
            self.add(value)
        elif len(nodes) == 1:
            if isinstance(value, ConfigNode):
                self.add(value)
            else:
                self.add(OrderedDict([(nodes[0], value)]))
        else:
            next_obj = nodes[0]
            new_path = "/".join(nodes[1:])
            self.attributes[next_obj].set(new_path, value)

    def get_path(self) -> str:
        """
        Get this node's path from the root
        :return: string with full path to this node
        """
        if self.parent is None:
            return f"/{self.name}" if self.name != "root" else ""
        return f"{self.parent.get_path()}/{self.name}"

    def print_fmt(self) -> None:
        """
        Print formatted node content to stdout
        """
        indent: str = "\t"
        print(
            f"{indent * (self.depth - 1)}[{self.name}] : (Type:{self.node_type},",
            f"Parent:{self.parent.name if self.parent else '/'}, Depth:{self.depth})",
        )
        for attribute in self.attributes.values():
            attribute.print_fmt()

    def search(
        self,
        path: str,
        name: str,
        criteria: Callable[[Any], bool],
        depth: int = 1,
        recursive: bool = False,
    ) -> List[str]:
        """
        Search this and underlying nodes
        :param path: path to start the search at
        :param name: name of the attribute to look for
        :param criteria: a function taking value of an attribute as argument,
            returning True if attribute value is a match
        :param depth:  Whether to return this node as match, if results are found in child nodes and to what depth.
            1 means search only this node
        :param recursive: search using this same set of parameters in all child nodes and downward
        :return: list of paths that match search criteria
        """
        return list(
            {
                os.path.join(path, result)
                for element in self._get_obj(path).attributes.values()
                if isinstance(element, ConfigNode)
                for result in element._search_here(name, criteria, depth, recursive)
            }
        )

    def _search_here(
        self, name: str, criteria: Callable[[Any], bool], depth: int, recursive: bool
    ) -> List[str]:
        results: List[str] = []
        for a_name, attribute in self.attributes.items():
            if isinstance(attribute, ConfigNode):
                if recursive:
                    results += [
                        os.path.join(self.name, result)
                        for result in attribute._search_here(
                            name, criteria, depth, recursive
                        )
                    ]
                if (
                    depth > 1
                    and len(attribute._search_here(name, criteria, depth - 1, False))
                    > 0
                ):
                    results.append(self.name)
            else:
                if name:
                    if name != a_name:
                        continue
                if criteria(attribute.value):
                    results.append(self.name)
        return results

    def list_nodes(self, path: str = "/", fullpath: bool = False) -> List[str]:
        """
        List child nodes
        :param path: path to a node. defaults to this node
        :param fullpath: return just names, or full paths
        :return: list of child node names
        """
        result: List[str] = [
            x.name
            for x in self._get_obj(path).attributes.values()
            if isinstance(x, ConfigNode)
        ]
        if fullpath:
            result = [os.path.join(path, x) for x in result]
        return result

    def list_attributes(self, path: str = "/", fullpath: bool = False) -> List[str]:
        """
        List node's attributes
        :param path: path to a node. defaults to this node
        :param fullpath: return just names, or full paths
        :return: list of attribute names
        """
        result: List[str] = [
            x.name
            for x in self._get_obj(path).attributes.values()
            if isinstance(x, ConfigAttribute)
        ]
        if fullpath:
            result = [os.path.join(path, x) for x in result]
        return result

    def set_node_type(self, new_value: str) -> None:
        """
        Set node_type property for this node
        :param new_value: C (content), CN (content node), AN (abstract node)
        """
        # On no change do nothing
        if self.node_type != new_value:
            # There are 3 possible propagation actions:
            # 1. Change attribute of child nodes
            # 2. Change attribute of parent nodes
            # 3. Verify existence of Content Node in parent node hierarchy
            child_value: Optional[str] = None
            parent_value: Optional[str] = None
            parent_check_cn: bool = False
            if self.node_type == "C" and new_value == "CN":
                # Nothing to do downward, all child nodes are already C
                # Change all upward nodes to Abstract Node (AN)
                parent_value = "AN"
            elif self.node_type == "C" and new_value == "AN":
                # Change immediate child nodes to Content Nodes (CN)
                # Change all upward nodes to Abstract Node (AN)
                child_value = "CN"
                parent_value = "AN"
            elif self.node_type == "CN" and new_value == "AN":
                # Change immediate child nodes to Content Nodes (CN)
                # Nothing to do upward, all parent nodes are already AN
                child_value = "CN"
            elif self.node_type == "AN" and new_value == "CN":
                # Change immediate child nodes to Content (C)
                # Nothing to do upward, all parent nodes are already AN
                child_value = "C"
            elif self.node_type == "CN" and new_value == "C":
                # Nothing to do downward, all child nodes are already C
                # Verify that there is a CN node upward, raise exception otherwise
                parent_check_cn = True
            elif self.node_type == "AN" and new_value == "C":
                # Change immediate child nodes to Content (C)
                # Verify that there is a CN node upward, raise exception otherwise
                child_value = "C"
                parent_check_cn = True

            self.node_type = new_value
            if parent_check_cn:
                # Verify existence of Content Node in parent node hierarchy
                cur_node: Optional["ConfigNode"] = self.parent
                found_cn: bool = False
                while cur_node is not None:
                    if cur_node.node_type == "CN":
                        found_cn = True
                        break
                    cur_node = cur_node.parent
                if not found_cn:
                    raise AttributeError(
                        f"Attempt to change {self.name} to a Content-only node with no Content Node parent"
                    )

            if parent_value:
                # Change attribute of parent nodes
                cur_node = self.parent
                while cur_node is not None:
                    cur_node.set_node_type(parent_value)
                    cur_node = cur_node.parent

            if child_value:
                # Change attribute of child nodes
                for child in self.attributes.values():
                    if isinstance(child, ConfigNode):
                        child.set_node_type(child_value)

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

    def _copy(self, node: "ConfigNode") -> None:
        """
        Copy all attributes from another ConfigNode
        :param node: Node to copy from
        """
        self.node_type = node.node_type
        self.attributes = node.attributes
        self.depth = node.depth
        self.name = node.name
        self.parent = node.parent
        self.template_gen = node.template_gen
        self.zk_path = node.zk_path


class ConfigAttribute:
    """
    Configuration attribute class
    """

    def __init__(
        self, name: str, value: Any, parent: Optional[ConfigNode] = None
    ) -> None:
        self.name: str = name
        self.value: Any = value
        self.parent: Optional[ConfigNode] = parent

    def _set_parent(self, parent_node: ConfigNode) -> None:
        self.parent = parent_node

    def _to_dict(self) -> Any:
        return self.value

    def _get_obj(self, path: Optional[str] = None) -> Any:
        return self

    def get_path(self) -> str:
        """
        Get this attribute's path from the root
        :return: string with full path to this attribute
        """
        if self.parent is None:
            return self.name
        else:
            return self.parent.get_path() + "/" + self.name

    def print_fmt(self) -> None:
        """
        Print formatted attribute to stdout
        """
        indent = "\t"
        print(
            f"{indent * self.parent.depth}{self.name} = {self.value} ({type(self.value)})"
        )


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
    global _zk_conn
    global _cfg_root

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
                auth_data=[("digest", src.username + ":" + src.password)],
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
            template_gen: Callable[
                ["ConfigNode"], "Type[TemplateNodeBase]"
            ] = literal_eval("module." + str(args.template).split(":", maxsplit=2)[1])
            init(args.source, template_gen=template_gen)
        else:
            init(args.source)

    if args.action == "ls":
        _cfg_root.print_fmt()
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
                auth_data=[("digest", dest.username + ":" + dest.password)],
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
                        json.dumps(_cfg_root.get(), default=utils.json_serial),
                    )
                else:
                    print("Destination node already exists, use --force to overwrite")
            else:
                zk_d.create(
                    dest.path,
                    json.dumps(_cfg_root.get(), default=utils.json_serial),
                    makepath=True,
                )
            zk_d.stop()
        else:
            if len(args.dest) > 5 and args.dest[-5:] == ".json":
                _cfg_root.write().json(args.dest)
            else:
                _cfg_root.write().cfg(args.dest)
    if args.source:
        src: urlparse.ParseResult = urlparse.urlparse(args.source)
        if src.scheme == "zookeeper":
            zk_s.stop()


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
            raise IOError(f"Empty configuration file {file_name}")
        return conf
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


if __name__ == "__main__":
    main()
