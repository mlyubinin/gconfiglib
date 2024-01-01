""" Configuration Node class."""

import os
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from gconfiglib.config_abcs import ConfigNodeABC, ConfigObject
from gconfiglib.config_attribute import ConfigAttribute
from gconfiglib.enums import NodeType


class ConfigNode(ConfigNodeABC):
    """
    Configuration node class
    """

    def __init__(
        self,
        name: str,
        parent: Optional["ConfigNode"] = None,
        attributes: Optional[
            ConfigObject | Dict[Any, Any] | List[ConfigObject | Tuple[str, Any]]
        ] = None,
        node_type: NodeType = NodeType.C,
        template_gen: Optional[
            Callable[["ConfigNode"], "Type[TemplateNodeBase]"]
        ] = None,
    ) -> None:
        """
        Initialization of ConfigNode can include:
        1. Explicit assignment of attributes by passing another ConfigNode, ConfigAttribute, a dictionary, or a list
        of any of the above
        2. Delayed assignment, which would create an empty node

        Node can also be immediately validated against a template, by passing template generator function.
        This will assign default values to any attributes missing in either explicit assignment or in configuration file

        :param name: Node name
        :param parent: Node's parent
        :param attributes: Node content
        :param node_type: Node type: C (content), CN (content node), AN (abstract node)
        :param template_gen: Function that takes configuration as parameter and generates the validation template

        """
        self.name: str = name
        self.parent: Optional["ConfigNode"] = parent
        self.node_type: NodeType = node_type
        self.zk_path: Optional[str] = None
        self.template_gen: Optional[
            Callable[["ConfigNode"], "Type[TemplateNodeBase]"]
        ] = None

        self.depth: int = 1
        if parent:
            self.depth = parent.depth + 1

        self.attributes: OrderedDict[str, ConfigObject] = OrderedDict()

        if attributes:
            # Explicitly set attributes
            self.add(attributes)

        if template_gen:
            # Validate configration
            self.template_gen = template_gen
            template: Type[TemplateNodeBase] = template_gen(self)
            if isinstance(template, TemplateNodeFixed) and template.name == "root":
                self._copy(template.validate(self))

    def add(
        self,
        attributes: ConfigObject
        | Dict[Any, Any]
        | List[ConfigObject | Tuple[str, Any]],
    ) -> None:
        """
        Add content to a node
        :param attributes: Can be ConfigNode, ConfigAttribute, a dictionary, or a list of any of the above
        """
        if isinstance(attributes, ConfigObject):
            # A single ConfigNode or ConfigAttribute
            self.attributes[attributes.name] = attributes
            attributes._set_parent(self)
        elif isinstance(attributes, list):
            # List of items
            for attribute in attributes:
                if isinstance(attribute, ConfigObject):
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
        self.set_node_type(self.node_type, force=True)

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

    def __str__(self) -> str:
        """
        Get formatted node content
        """
        indent: str = "\t"
        result: str = f"{indent * (self.depth - 1)}[{self.name}] : (Type:{self.node_type.name},Parent:{self.parent.name if self.parent else '/'}, Depth:{self.depth})"
        for attribute in self.attributes.values():
            result += str(attribute)
        return result

    def __repr__(self) -> str:
        """Get string representation of the dictionary object

        Returns:
            str: OrderedDict object converted to string
        """
        return self._to_dict().__repr__()

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

    def set_node_type(self, new_value: NodeType, force: bool = False) -> None:
        """
        Set node_type property for this node
        :param new_value: C (content), CN (content node), AN (abstract node)
        :param force: boolean. Force the change to parent and child nodes even if current node type is unchanged
        """
        # Actions on node type change
        if self.node_type != new_value:
            # There are 3 possible propagation actions:
            # 1. Change attribute of child nodes
            # 2. Change attribute of parent nodes
            # 3. Verify existence of Content Node in parent node hierarchy
            child_value: Optional[NodeType] = None
            parent_value: Optional[NodeType] = None
            parent_check_cn: bool = False
            if self.node_type == NodeType.C and new_value == NodeType.CN:
                # Nothing to do downward, all child nodes are already C
                # Change all upward nodes to Abstract Node (AN)
                parent_value = NodeType.AN
            elif self.node_type == NodeType.C and new_value == NodeType.AN:
                # Change immediate child nodes to Content Nodes (CN)
                # Change all upward nodes to Abstract Node (AN)
                child_value = NodeType.CN
                parent_value = NodeType.AN
            elif self.node_type == NodeType.CN and new_value == NodeType.AN:
                # Change immediate child nodes to Content Nodes (CN)
                # Nothing to do upward, all parent nodes are already AN
                child_value = NodeType.CN
            elif self.node_type == NodeType.AN and new_value == NodeType.CN:
                # Change immediate child nodes to Content (C)
                # Nothing to do upward, all parent nodes are already AN
                child_value = NodeType.C
            elif self.node_type == NodeType.CN and new_value == NodeType.C:
                # Nothing to do downward, all child nodes are already C
                # Verify that there is a CN node upward, raise exception otherwise
                parent_check_cn = True
            elif self.node_type == NodeType.AN and new_value == NodeType.C:
                # Change immediate child nodes to Content (C)
                # Verify that there is a CN node upward, raise exception otherwise
                child_value = NodeType.C
                parent_check_cn = True

            self.node_type = new_value
            if parent_check_cn:
                # Verify existence of Content Node in parent node hierarchy
                cur_node: Optional["ConfigNode"] = self.parent
                found_cn: bool = False
                while cur_node is not None:
                    if cur_node.node_type == NodeType.CN:
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
        else:
            # On force update
            if self.node_type == NodeType.AN:
                # Change parent node type to AN
                # Change child node type to CN if it's C
                if self.parent:
                    self.parent.set_node_type(NodeType.AN)
                for child in self.attributes.values():
                    if isinstance(child, ConfigNode) and child.node_type == NodeType.C:
                        child.set_node_type(NodeType.CN)
            elif self.node_type == NodeType.CN:
                # Change parent node type to AN
                # Change child node type to C
                if self.parent:
                    self.parent.set_node_type(NodeType.AN)
                for child in self.attributes.values():
                    if isinstance(child, ConfigNode):
                        child.set_node_type(NodeType.C)
            else:
                # (node type is C)
                # Change parent node type to CN if it's AN
                # Change child node type to C
                if self.parent and self.parent.node_type == NodeType.AN:
                    self.parent.set_node_type(NodeType.CN)
                for child in self.attributes.values():
                    if isinstance(child, ConfigNode):
                        child.set_node_type(NodeType.C)

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
