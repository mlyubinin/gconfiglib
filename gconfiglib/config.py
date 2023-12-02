# Copyright (C) 2019 Michael Lyubinin
# Author: Michael Lyubinin
# Contact: michael@lyubinin.com

""" Gconfiglib enhanced configuration library. """

import argparse
import collections
import datetime as dt
import importlib
import json
import logging
import os
import sys
from urllib import parse as urlparse

from kazoo.client import KazooClient
from kazoo.security import make_digest_acl

from gconfiglib import utils

# Internal module variables. Values are assigned at runtime
# Root configuration node
_cfg_root = None
# Zookeeper connection (if using zookeeper)
_zk_conn = None
# Zookeeper update flag (used for managing update collisions in triggers)
_zk_update = False


def get(path=None):
    """
    Get configuration as OrderedDict starting at specific path relative to root
    :param path: path from root, str. If None, treat as '/'
    :return: OrderedDict if path ends in a node, otherwise value of an attribute at path
    :raises: AttributeError if configuration has not been initialized
    """
    global _cfg_root
    return _cfg_root.get(path)


def set(path, value):
    """
    Assign new value to existing node/attribute or add a new node/attribute at path
    :param path: path from root, str.
    :param value: Can be ConfigNode, ConfigAttribute, dictionary, or a list any of the above to assign at node path, or
                    any other value to set an attribute value
    :raises: AttributeError if configuration has not been initialized
    """
    global _cfg_root
    _cfg_root.set(path, value)


def root():
    """
    Return root configuration object or None if it's not initialized
    """
    global _cfg_root
    return _cfg_root


def init(filename=None, default_paths=None, default_env_path=None, template_gen=None):
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
    candidate_list = []
    if filename:
        candidate_list.append(filename)
    if default_env_path and default_env_path in os.environ:
        candidate_list.append(os.environ[default_env_path])
    if default_paths and isinstance(default_paths, list):
        candidate_list.extend(default_paths)

    # Attempt to read candidates, fail if explicit file is invalid or if no configuration found
    for fname in candidate_list:
        try:
            logger.log(logging.DEBUG, "Trying to read configuration from %s" % fname)
            candidate_uri = urlparse.urlparse(fname)
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
                            logger.log(
                                logging.DEBUG,
                                "Refreshing configuration to version %s" % stat.version,
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
                                logger.log(
                                    logging.DEBUG,
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
        except:
            if filename and filename == fname:
                raise Exception("Could not read configuration file " + fname)
            else:
                continue
        logger.log(logging.DEBUG, "Read configuration from %s" % fname)
        if _cfg_root is not None:
            break

    # Validate configration
    if template_gen:
        _cfg_root.template_gen = template_gen
        template = template_gen(_cfg_root)
        if isinstance(template, TemplateNodeFixed) and template.name == "root":
            _cfg_root = template.validate(_cfg_root)
        else:
            logger.log(logging.ERROR, "Invalid configuration template")
    if _cfg_root is None:
        logger.log(logging.CRITICAL, "Could not initialize configuration")
        raise Exception("Could not initialize configuration")


class TemplateBase(object):
    """
    Common elements for all template objects
    """

    def __init__(self, optional=True, validator=None, description=None):
        self.optional = optional
        self.validator = validator
        self.description = description


class TemplateAttributeBase(TemplateBase):
    """
    Base attribute template
    """

    def __init__(
        self,
        optional=True,
        value_type=str,
        validator=None,
        default_value=None,
        description=None,
    ):
        """
        :param optional: Is this attribute optional (True) or mandatory (False)
        :param value_type: Value type (int, str, etc.)
        :param validator: Validator function. Should take value as argument and return new, possibly changed value
                            Raises ValueError on any validation failure
        :param default_value: Value to assign if missing from configuration object
        :param description: Attribute description (used when generating sample configuration files)
        """
        self.value_type = value_type
        self.default_value = default_value
        super(TemplateAttributeBase, self).__init__(optional, validator, description)

    def validate(self, value, name):
        """
        Validate an attribute
        :param value: Value to be validated
        :param name: Name of the attribute to be validated
        :return: validated value (possibly changed from original), or raises ValueError on failure to validate
        """
        if value is None:
            value = ConfigAttribute(name, self.default_value)
        elif value.value is not None and type(value.value) != self.value_type:
            try:
                value.value = self.value_type(value.value)
            except:
                if self.value_type == dt.date and type(value.value) == dt.datetime:
                    value.value = value.value.date()
                else:
                    if value.value is not None and value.value != "":
                        raise ValueError(
                            "Expecting %s to be of type %s"
                            % (value.get_path(), self.value_type)
                        )

        if self.validator is not None:
            if not self.optional or value.value is not None:
                try:
                    valid = self.validator(value.value)
                    problem = ""
                except:
                    valid = False
                    problem = sys.exc_info()[0]
                if not valid:
                    message = "Parameter %s failed validation for value %s" % (
                        value.get_path(),
                        value.value,
                    )
                    if problem != "":
                        message += ": %s" % problem
                    raise ValueError(message)
        if not self.optional and value.value is None:
            # mandatory attribute with no value and no default
            raise ValueError(
                "Mandatory parameter %s has not been set, and has no default value"
                % value.get_path()
            )

        if value.value is None:
            return None
        else:
            return value

    def sample(self, format="JSON"):
        """
        Generate a line for sample configuration file
        :param format: JSON or TEXT
        :return: string
        """
        value = self.default_value if self.default_value is not None else ""
        description = self.description if self.description is not None else ""
        try:
            name = self.name
        except AttributeError:
            name = "Attribute"
        if format == "JSON":
            return '"%s" : %s' % (
                name,
                json.dumps(value, ensure_ascii=True, default=utils.json_serial),
            )
        elif format == "TEXT":
            return "#\n# %s\n# %s = %s\n" % (description, name, value)


class TemplateAttributeFixed(TemplateAttributeBase):
    """
    Configuration attribute template class
    For an attribute with a fixed name
    """

    def __init__(
        self,
        name,
        optional=True,
        value_type=str,
        validator=None,
        default_value=None,
        description=None,
    ):
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
        super(TemplateAttributeFixed, self).__init__(
            optional, value_type, validator, default_value, description
        )

    def validate(self, value):
        """
        Validate an attribute
        :param value: Value to be validated
        :return: validated value (possibly changed from original), or raises ValueError on failure to validate
        """
        return super(TemplateAttributeFixed, self).validate(value, self.name)


class TemplateAttributeVariable(TemplateAttributeBase):
    """
    Variable configuration attribute template class
    For an attribute with a name not known until runtime
    """

    def __init__(self, value_type=str, validator=None, description=None):
        """
        :param value_type: Value type (int, str, etc.)
        :param validator: Validator function. Should take value as argument and return new, possibly changed value
                            Raises ValueError on any validation failure
        :param description: Attribute description (used when generating sample configuration files)
        """
        super(TemplateAttributeVariable, self).__init__(
            True, value_type, validator, description=description
        )

    def validate(self, value, name):
        """
        Validate an attribute
        :param value: Value to be validated
        :param name: Name of the attribute to be validated
        :return: validated value (possibly changed from original), or raises ValueError on failure to validate
        """
        return super(TemplateAttributeVariable, self).validate(value, name)


class TemplateNodeBase(TemplateBase):
    """
    Node template base class
    """

    def __init__(
        self, name, optional=True, validator=None, description=None, node_type=None
    ):
        """
        :param name: Node name
        :param optional: Is this node optional (True) or mandatory (False)
        :param validator: Validator function. Should take value as argument and return new, possibly changed value
                            Raises ValueError on any validation failure
        :param description: Node description (used when generating sample configuration files)
        :param node_type: Node type: C (content), CN (content node), AN (abstract node)
        """
        self.name = name
        self.attributes = collections.OrderedDict()
        if node_type in ["C", "CN", "AN"]:
            self.node_type = node_type
        else:
            self.node_type = None
        super(TemplateNodeBase, self).__init__(optional, validator, description)

    def validate(self, node):
        """
        Validate a node
        :param node: Node to be validated
        :return: validated node (possibly changed from original), or raises ValueError on failure to validate
        """
        if node is None:
            # Empty node - try creating a node if it's mandatory, otherwise return None
            if self.optional:
                return None
            else:
                node = ConfigNode(self.name)
        elif not isinstance(node, ConfigNode):
            raise ValueError(
                "Configuration object passed for validation to template %s is not a ConfigNode"
                % self.name
            )
        elif len(self.attributes) == 0:
            raise ValueError("Template for node %s has no attributes" % self.name)
        if self.validator is not None:
            try:
                valid = self.validator(node.get())
                problem = ""
            except:
                valid = False
                problem = sys.exc_info()[0]
            if not valid:
                message = "Node %s failed validation" % node.get_path()
                if problem != "":
                    message += ": %s" % problem
                raise ValueError(message)
        if self.node_type and self.node_type != node.node_type:
            node.set_node_type(self.node_type)
        return node

    def sample(self, format="JSON"):
        """
        Generate a line for sample configuration file
        :param format: JSON or TEXT
        :return: string
        """
        description = self.description if self.description else ""
        if format == "JSON":
            if self.name == "root":
                result = "{"
            else:
                result = '"%s" : {' % self.name
            for attribute in self.attributes.values():
                result += attribute.sample(format) + ", "
            result = result[:-2] + "}"
        elif format == "TEXT":
            if self.name == "root":
                result = ""
            else:
                result = "# %s\n# [%s]\n" % (description, self.name)
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

    def __init__(
        self, name, optional=True, validator=None, description=None, node_type=None
    ):
        """
        :param name: Node name
        :param optional: Is this node optional (True) or mandatory (False)
        :param validator: Validator function. Should take value as argument and return new, possibly changed value
                            Raises ValueError on any validation failure
        :param description: Node description (used when generating sample configuration files)
        :param node_type: Node type: C (content), CN (content node), AN (abstract node)
        """
        super(TemplateNodeFixed, self).__init__(
            name, optional, validator, description, node_type
        )

    def add(self, attr):
        """
        Add child nodes/attribute templates
        :param attr: Any TemplateBase descendant object
        """
        if isinstance(attr, TemplateBase):
            if attr.name in self.attributes.keys():
                raise ValueError(
                    "Attribute or node %s can only be added to node %s once"
                    % (attr.name, self.name)
                )
            else:
                self.attributes[attr.name] = attr
        else:
            raise ValueError(
                "Attempt to add invalid attribute type to %s template" % self.name
            )

    def validate(self, node):
        """
        Validate a node
        :param node: Node to be validated
        :return: validated node (possibly changed from original), or raises ValueError on failure to validate
        """
        node = super(TemplateNodeFixed, self).validate(node)
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
                if isinstance(attr_t, TemplateAttributeBase) and attr_t_name not in [
                    x.name for x in node.attributes.values()
                ]:
                    test_attr = ConfigAttribute(
                        attr_t_name, value=attr_t.default_value, parent=node
                    )
                else:
                    test_attr = None

                new_value = attr_t.validate(
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
                "Mandatory node %s is missing, with no defaults set" % node.get_path()
            )
        if len(node.attributes) == 0:
            return None
        else:
            return node


class TemplateNodeVariableAttr(TemplateNodeBase):
    """
    Configuration Node with a variable list of attributes
    """

    def __init__(
        self,
        name,
        attr,
        optional=True,
        validator=None,
        description=None,
        node_type=None,
    ):
        """
        :param name: Node name
        :param attr: Attribute template. All attributes in VariableAttr node must be of the same type
        :param optional: Is this node optional (True) or mandatory (False)
        :param validator: Validator function. Should take value as argument and return new, possibly changed value
                            Raises ValueError on any validation failure
        :param description: Node description (used when generating sample configuration files)
        :param node_type: Node type: C (content), CN (content node), AN (abstract node)
        """
        super(TemplateNodeVariableAttr, self).__init__(
            name, optional, validator, description, node_type
        )
        if not isinstance(attr, TemplateAttributeVariable):
            raise ValueError(
                "Attempt to add invalid attribute type to %s template. This node can contain only one TemplateAttributeVariable attribute template and nothing else"
                % self.name
            )
        self.attributes["variable_attribute"] = attr

    def validate(self, node):
        """
        Validate a node
        :param node: Node to be validated
        :return: validated node (possibly changed from original), or raises ValueError on failure to validate
        """
        node = super(TemplateNodeVariableAttr, self).validate(node)
        # If None, pass it back without further checks (missing optional node was not created)
        if node is None:
            return None
        if len(node.attributes) == 0 and not self.optional:
            raise ValueError("Node %s cannot be empty" % node.get_path())
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
        else:
            return node


class TemplateNodeSet(TemplateNodeBase):
    """
    Template class for a set of either fixed or variable attribute nodes
    """

    def __init__(self, name, node, names_lst):
        """
        :param name: Nodeset name
        :param attr: Node template. All nodes in NodeSet node must be of the same type
        :param names_lst: List of names of nodes that should be in the node set
        """
        super(TemplateNodeSet, self).__init__(name)
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

    def validate(self, node):
        """
        Validate a node
        :param node: Node to be validated
        :return: validated node (possibly changed from original), or raises ValueError on failure to validate
        """
        logger = logging.getLogger("gconfiglib")

        node = super(TemplateNodeSet, self).validate(node)
        # If None, pass it back without further checks (missing optional node was not created)
        if node is None:
            return None
        for name in self.names_lst:
            if name not in node.attributes.keys():
                if not self.attributes["node"].optional:
                    node.add(ConfigNode(name))
                    logger.log(
                        logging.DEBUG,
                        "Mandatory node %s is missing in %s" % (name, node.get_path()),
                    )
                else:
                    logger.log(
                        logging.DEBUG,
                        "Optional node %s is missing in %s" % (name, node.get_path()),
                    )
                    continue
            new_value = self.attributes["node"].validate(node._get_obj(name))
            if isinstance(new_value, ConfigAttribute) and new_value.value is not None:
                node.add(new_value)
            elif isinstance(new_value, ConfigNode) and len(new_value.attributes) > 0:
                node.add(new_value)
        if len(node.attributes) == 0:
            return None
        else:
            return node

    def sample(self, format="JSON"):
        """
        Generate a line for sample configuration file
        :param format: JSON or TEXT
        :return: string
        """
        description = (
            self.attributes["node"].description
            if self.attributes["node"].description
            else ""
        )
        for node_name in self.names_lst:
            if format == "JSON":
                result = '"%s" : {' % node_name
                for attribute in self.attributes["node"].attributes.values():
                    result += attribute.sample(format) + ", "
                result = result[:-2] + "}, "
            elif format == "TEXT":
                result = "# %s\n# [%s]\n" % (description, node_name)
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


class ConfigReader(object):
    """
    Configuration file reader
    """

    def __init__(self):
        pass

    def _check_file(self, filename):
        """
        Internal method. Checks that the file exists
        :param filename: Filename to check
        :return: True is file exists and is readable
        """
        if os.path.isfile(filename) and os.access(filename, os.R_OK):
            return True
        else:
            raise IOError("File %s does not exist or is nor readable" % filename)

    def cfg(self, filename):
        """
        Reader for plain text config files
        :param filename: Filename
        :return: ConfigNode with file contents parsed into nodes and attributes
        """
        if self._check_file(filename):
            return ConfigNode("root", attributes=read_config(filename), node_type="CN")

    def json(self, filename):
        """
        Reader for json config files
        :param filename: Filename
        :return: ConfigNode with file contents parsed into nodes and attributes
        """
        if self._check_file(filename):
            with open(filename, "r") as f:
                return ConfigNode(
                    "root",
                    attributes=json.load(f, object_pairs_hook=utils.json_decoder),
                    node_type="CN",
                )

    def zk(self, path, name="root"):
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
        elif not _zk_conn.exists(path):
            logger.log(logging.ERROR, "Path %s does not exist" % path)
        else:
            children = _zk_conn.get_children(path)
            try:
                node = ConfigNode(
                    name,
                    attributes=json.loads(
                        _zk_conn.get(path)[0], object_pairs_hook=utils.json_decoder
                    ),
                    node_type="CN",
                )
            except ValueError as e:
                if e.message == "No JSON object could be decoded" and len(children) > 0:
                    node = ConfigNode(name)
                else:
                    raise
            node.zk_path = path
            if len(children) > 0:
                node.set_node_type("AN")
                for child in children:
                    node.add(ConfigNode.read().zk(path + "/" + child, child))
            return node
        return None


class ConfigWriter(object):
    """
    Configuration file writer
    """

    def __init__(self, cfg_obj):
        self.cfg_obj = cfg_obj

    def _check_file(self, filename, force):
        """
        Internal method. Checks that the file exists
        :param filename: Filename to check
        :param force: Force file overwrite (True/False)
        :return: True if file does not exist, or if force is set to True. Raises IOError otherwise
        """
        if os.path.isfile(filename) and not force:
            raise IOError("File %s exists" % filename)
        return True

    def cfg(self, filename, force=False):
        """
        Writer for plain text config files
        :param filename: Filename
        :param force: Force file overwrite (True/False)
        """
        try:
            self._check_file(filename, force)
        except IOError as e:
            raise IOError("Failed to open the file %s" % filename, e)
        with open(filename, mode="w") as f:
            for node in self.cfg_obj.attributes.values():
                if isinstance(node, ConfigAttribute):
                    raise ValueError(
                        "cfg format does not support attributes at root level"
                    )
                f.write("\n[%s]\n" % node.name)
                for attribute in node.attributes.values():
                    if isinstance(attribute, ConfigNode):
                        raise ValueError(
                            "cfg format does not support multi-level hierarchy"
                        )
                    f.write("%s = %s\n" % (attribute.name, str(attribute.value)))

    def json(self, filename, force=False):
        """
        Writer for json config files
        :param filename: Filename
        :param force: Force file overwrite (True/False)
        """
        try:
            self._check_file(filename, force)
        except IOError as e:
            raise IOError("Failed to open the file %s" % filename, e)
        with open(filename, mode="w") as f:
            json.dump(
                self.cfg_obj.get(),
                f,
                ensure_ascii=True,
                indent=4,
                default=utils.json_serial,
                separators=(",", ": "),
            )

    def zk(self, path=None, force=False):
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
            _zk_update = True
            if path is None:
                path = self.cfg_obj.zk_path

            if self.cfg_obj.node_type == "C":
                raise AttributeError(
                    "write method called on Content node %s" % self.cfg_obj.name
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
                        path + "/" + node_name, force=force
                    )
                    _zk_update = True
            _zk_update = False


class ConfigNode(object):
    """
    Configuration node class
    """

    def __init__(
        self,
        name,
        parent=None,
        attributes=None,
        node_type="C",
        filename=None,
        default_paths=None,
        default_env_path=None,
        template_gen=None,
    ):
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
        self.name = name
        self.parent = parent
        self.node_type = node_type
        self.zk_path = None
        self.template_gen = None

        logger = logging.getLogger("gconfiglib")

        if parent:
            self.depth = parent.depth + 1
        else:
            self.depth = 1

        self.attributes = collections.OrderedDict()

        # Form candidate list
        candidate_list = []
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
                    logger.log(
                        logging.DEBUG, "Trying to read configuration from %s" % fname
                    )
                    candidate_uri = urlparse.urlparse(fname)
                    if candidate_uri.scheme == "zookeeper":
                        if not _zk_conn:
                            _zk_conn = utils.zk_connect(fname)
                        self._copy(ConfigNode.read().zk(candidate_uri.path))
                    elif len(fname) > 5 and fname[-5:] == ".json":
                        self._copy(ConfigNode.read().json(fname))
                    else:
                        self._copy(ConfigNode.read().cfg(fname))
                except:
                    continue

                if len(self.attributes) > 0:
                    # Configuration has been read successfully
                    break

            if len(self.attributes) == 0:
                raise Exception(
                    "Could not read configuration file from any of the specified sources: %s"
                    % candidate_list
                )

        if template_gen:
            # Validate configration
            self.template_gen = template_gen
            template = template_gen(self)
            if isinstance(template, TemplateNodeFixed) and template.name == "root":
                self._copy(template.validate(self))

    def add(self, attributes):
        """
        Add content to a node
        :param attributes: Can be ConfigNode, ConfigAttribute, a dictionary, or a list of any of the above
        """
        if isinstance(attributes, ConfigNode) or isinstance(
            attributes, ConfigAttribute
        ):
            self.attributes[attributes.name] = attributes
            attributes._set_parent(self)
        elif isinstance(attributes, list):
            for attribute in attributes:
                if isinstance(attribute, ConfigNode) or isinstance(
                    attribute, ConfigAttribute
                ):
                    self.attributes[attribute.name] = attribute
                elif isinstance(attribute, tuple) and len(attribute) == 2:
                    if isinstance(attribute[1], dict) or isinstance(attribute[1], list):
                        self.attributes[attribute[0]] = ConfigNode(
                            attribute[0], parent=self, attributes=attribute[1]
                        )
                    else:
                        self.attributes[attribute[0]] = ConfigAttribute(
                            attribute[0], attribute[1], parent=self
                        )
                else:
                    raise ValueError(
                        "ConfigNode.add only accepts single ConfigNode, ConfigAttribute, or a list of ConfigNodes and/or ConfigAttributes"
                    )
        elif isinstance(attributes, dict):
            for a_key, a_value in attributes.items():
                if isinstance(a_value, dict):
                    self.attributes[a_key] = ConfigNode(
                        a_key, parent=self, attributes=a_value
                    )
                else:
                    self.attributes[a_key] = ConfigAttribute(
                        a_key, a_value, parent=self
                    )
        else:
            raise ValueError(
                "ConfigNode.add only accepts single ConfigNode, ConfigAttribute, or a list of ConfigNodes and/or ConfigAttributes"
            )

    def delete(self, path):
        """
        Delete content
        :param path: Path - everything at and below this path will be deleted
        """
        nodes = [x for x in path.split("/") if x != ""]
        if len(nodes) == 0:
            raise ValueError("No path to delete specified")
        elif len(nodes) == 1:
            self.attributes.pop(nodes[0], None)
        else:
            new_path = "/".join(nodes[1:])
            return self.attributes[nodes[0]].delete(new_path)

    def _set_parent(self, parent_node):
        """
        Set node's parent
        :param parent_node: parent ConfigNode
        """
        self.parent = parent_node
        self.depth = parent_node.depth + 1
        for child in self.attributes.values():
            if isinstance(child, ConfigNode):
                child._set_parent(self)

    def _to_dict(self):
        """
        Convert to dictionary
        :return: Node's content converted to OrderedDict
        """
        result = collections.OrderedDict()
        for attribute_name, attribute_value in self.attributes.items():
            result[attribute_name] = attribute_value._to_dict()
        return result

    def _get_obj(self, path=None):
        """
        Internal method. Retrieve object at path
        :param path: Path
        :return: ConfigNode or ConfigAttribute at specified path
        """
        if not path:
            return self
        nodes = [x for x in path.split("/") if x != ""]
        if len(nodes) == 0:
            return self
        next_obj = nodes[0]
        new_path = "/".join(nodes[1:]) if len(nodes) > 1 else None
        if next_obj not in self.attributes.keys():
            return None
        return self.attributes[next_obj]._get_obj(new_path)

    def get(self, path=None):
        """
        Retrieve object at path as OrderedDict
        :param path: Path
        :return: OrderedDict
        """
        obj = self._get_obj(path)
        if obj is None:
            return None
        return obj._to_dict()

    def get_attributes(self, path=None):
        """
        Retrieve attributes of a node at path
        :param path: Path to a node
        :return: OrderedDict with 'attribute': 'value' pairs
        """
        if path:
            result = self._get_obj(path).get_attributes()
        else:
            result = collections.OrderedDict()
            for attribute_name, attribute_value in self.attributes.items():
                if isinstance(attribute_value, ConfigAttribute):
                    result[attribute_name] = attribute_value._to_dict()
        return result

    def set(self, path, value):
        """
        Add or update content
        :param path: Path to node or attribute to update
        :param value: Value to assign
        """
        nodes = [x for x in path.split("/") if x != ""]
        if len(nodes) == 0:
            self.add(value)
        elif len(nodes) == 1:
            if isinstance(value, ConfigNode):
                self.add(value)
            else:
                self.add(collections.OrderedDict([(nodes[0], value)]))
        else:
            next_obj = nodes[0]
            new_path = "/".join(nodes[1:])
            self.attributes[next_obj].set(new_path, value)

    def get_path(self):
        """
        Get this node's path from the root
        :return: string with full path to this node
        """
        if self.parent is None:
            return "/%s" % self.name if self.name != "root" else ""
        else:
            return self.parent.get_path() + "/" + self.name

    def print_fmt(self):
        """
        Print formatted node content to stdout
        """
        indent = "\t"
        print(
            "%s[%s] : (Type:%s, Parent:%s, Depth:%d)"
            % (
                indent * (self.depth - 1),
                self.name,
                self.node_type,
                self.parent.name if self.parent else "/",
                self.depth,
            )
        )
        for attribute in self.attributes.values():
            attribute.print_fmt()

    def search(self, path, name, criteria, depth=1, recursive=False):
        """
        Search this and underlying nodes
        :param path: path to start the search at
        :param name: name of the attribute to look for
        :param criteria: a function taking value of an attribute as argument, returning True if attribute value is a match
        :param depth:  Whether to return this node as match, if results are found in child nodes and to what depth. 1 means search only this node
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

    def _search_here(self, name, criteria, depth, recursive):
        results = []
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

    def list_nodes(self, path="/", fullpath=False):
        """
        List child nodes
        :param path: path to a node. defaults to this node
        :param fullpath: return just names, or full paths
        :return: list of child node names
        """
        result = [
            x.name
            for x in self._get_obj(path).attributes.values()
            if isinstance(x, ConfigNode)
        ]
        if fullpath:
            result = [os.path.join(path, x) for x in result]
        return result

    def list_attributes(self, path="/", fullpath=False):
        """
        List node's attributes
        :param path: path to a node. defaults to this node
        :param fullpath: return just names, or full paths
        :return: list of attribute names
        """
        result = [
            x.name
            for x in self._get_obj(path).attributes.values()
            if isinstance(x, ConfigAttribute)
        ]
        if fullpath:
            result = [os.path.join(path, x) for x in result]
        return result

    def set_node_type(self, new_value):
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
            child_value = None
            parent_value = None
            parent_check_cn = False
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
                cur_node = self.parent
                found_cn = False
                while cur_node is not None:
                    if cur_node.node_type == "CN":
                        found_cn = True
                        break
                    else:
                        cur_node = cur_node.parent
                if not found_cn:
                    raise AttributeError(
                        "Attempt to change %s to a Content-only node with no Content Node parent"
                        % self.name
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
    def read():
        """
        Generates a reader for this node
        :return: ConfigReader object
        """
        return ConfigReader()

    def write(self):
        """
        Generates a writer for this node
        :return: ConfigWriter object
        """
        return ConfigWriter(self)

    def _copy(self, node):
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


class ConfigAttribute(object):
    """
    Configuration attribute class
    """

    def __init__(self, name, value, parent=None):
        self.name = name
        self.value = value
        self.parent = parent

    def _set_parent(self, parent_node):
        self.parent = parent_node

    def _to_dict(self):
        return self.value

    def _get_obj(self, path=None):
        return self

    def get_path(self):
        """
        Get this attribute's path from the root
        :return: string with full path to this attribute
        """
        if self.parent is None:
            return self.name
        else:
            return self.parent.get_path() + "/" + self.name

    def print_fmt(self):
        """
        Print formatted attribute to stdout
        """
        indent = "\t"
        print(
            "%s%s = %s (%s)"
            % (
                indent * self.parent.depth,
                self.name,
                str(self.value),
                str(type(self.value)),
            )
        )


def main():
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
    args = parser.parse_args()
    cfgctl(args)


def cfgctl(args):
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
        src = urlparse.urlparse(args.source)
        if src.scheme == "zookeeper":
            zk_s = KazooClient(
                hosts=src.hostname,
                default_acl=[make_digest_acl(src.username, src.password, all=True)],
                auth_data=[("digest", src.username + ":" + src.password)],
            )
            zk_s.start()
            zk_s.ensure_path(src.password)
            logger.log(
                logging.DEBUG,
                "Connected to source Zookeeper at %s:%s" % (src.hostname, src.port),
            )

    if args.action in ["ls", "cp"] and args.source:
        if args.template:
            module = importlib.import_module(str(args.template).split(":")[0])
            template_gen = eval("module." + str(args.template).split(":")[1])
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
        dest = urlparse.urlparse(args.dest)
        if dest.scheme == "zookeeper":
            zk_d = KazooClient(
                hosts=dest.hostname,
                default_acl=[make_digest_acl(dest.username, dest.password, all=True)],
                auth_data=[("digest", dest.username + ":" + dest.password)],
            )
            zk_d.start()
            zk_d.ensure_path(dest.password)
            logger.log(
                logging.DEBUG,
                "Connected to destination Zookeeper at %s:%s"
                % (dest.hostname, dest.port),
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
        src = urlparse.urlparse(args.source)
        if src.scheme == "zookeeper":
            zk_s.stop()


def read_config(file_name):
    """
    Reads configuration from a file
    :param file_name: name of the configuration file
    :return: dictionary object with config key-value pairs
    :return: dictionary object with config key-value pairs
    """
    conf = collections.OrderedDict()

    cur_section = ""
    # Read the file
    if os.path.isfile(file_name) and os.access(file_name, os.R_OK):
        with open(file_name, "r") as config_file:
            config_data = config_file.readlines()
        # For every line:
        for line in config_data:
            config_key, config_value = parse_config_line(line)
            if config_key == 0:
                continue
            elif config_key == 1:
                cur_section = config_value
                conf[cur_section] = collections.OrderedDict()
                continue

            # Assign to section or sectionless
            if cur_section == "":
                conf[config_key] = config_value
            else:
                conf[cur_section][config_key] = config_value
        if conf == {}:
            raise Exception("Empty configuration file " + file_name)
        return conf
    else:
        raise Exception("File " + file_name + " does not exist or is not readable")


def parse_config_line(line):
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
    pair = line.split("=")
    if len(pair) != 2:
        # only the first '=' matters
        pair = [pair[0], "=".join(pair[1:])]
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
        lst = config_value[1 : len(config_value) - 1].split(",")
        for i, _ in enumerate(lst):
            lst[i] = lst[i].strip()
        config_value = lst

    return config_key, config_value


if __name__ == "__main__":
    main()
