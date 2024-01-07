"""Abstract configuration object class.
"""
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Tuple

from gconfiglib.enums import NodeType


class ConfigObject(ABC):
    """Abstract configuration object class."""

    name: str
    parent: Optional["ConfigNodeABC"]


class ConfigAttributeABC(ConfigObject):
    """
    Configuration attribute abstract class
    """

    value: Any

    @abstractmethod
    def _set_parent(self, parent_node: "ConfigNodeABC") -> None:
        pass

    @abstractmethod
    def _to_dict(self) -> Any:
        pass

    @abstractmethod
    def _get_obj(self, path: Optional[str] = None) -> Any:
        pass

    @abstractmethod
    def get_path(self) -> str:
        """
        Get this attribute's path from the root
        :return: string with full path to this attribute
        """


class ConfigNodeABC(ConfigObject):
    """
    Configuration node abstract class
    """

    attributes: OrderedDict[str, ConfigObject]
    node_type: NodeType
    zk_path: Optional[str]
    depth: int

    @abstractmethod
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

    @abstractmethod
    def delete(self, path: str) -> None:
        """
        Delete content
        :param path: Path - everything at and below this path will be deleted
        """

    @abstractmethod
    def _set_parent(self, parent_node: "ConfigNodeABC") -> None:
        """
        Set node's parent
        :param parent_node: parent ConfigNode
        """

    @abstractmethod
    def _to_dict(self) -> OrderedDict[str, Any]:
        """
        Convert to dictionary
        :return: Node's content converted to OrderedDict
        """

    @abstractmethod
    def _get_obj(self, path: Optional[str] = None) -> Optional[ConfigObject]:
        """
        Internal method. Retrieve object at path
        :param path: Path
        :return: ConfigNode or ConfigAttribute at specified path or None
        """

    @abstractmethod
    def get(self, path: Optional[str] = None) -> Optional[OrderedDict[str, Any] | Any]:
        """
        Retrieve object at path as OrderedDict
        :param path: Path
        :return: OrderedDict, attribute value or None
        """

    @abstractmethod
    def get_attributes(self, path: Optional[str] = None) -> OrderedDict[str, Any]:
        """
        Retrieve attributes of a node at path
        :param path: Path to a node
        :return: OrderedDict with 'attribute': 'value' pairs
        """

    @abstractmethod
    def set(self, path: str, value: Any) -> None:
        """
        Add or update content
        :param path: Path to node or attribute to update
        :param value: Value to assign
        """

    @abstractmethod
    def get_path(self) -> str:
        """
        Get this node's path from the root
        :return: string with full path to this node
        """

    @abstractmethod
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

    @abstractmethod
    def _search_here(
        self, name: str, criteria: Callable[[Any], bool], depth: int, recursive: bool
    ) -> List[str]:
        pass

    @abstractmethod
    def list_nodes(self, path: str = "/", fullpath: bool = False) -> List[str]:
        """
        List child nodes
        :param path: path to a node. defaults to this node
        :param fullpath: return just names, or full paths
        :return: list of child node names
        """

    @abstractmethod
    def list_attributes(self, path: str = "/", fullpath: bool = False) -> List[str]:
        """
        List node's attributes
        :param path: path to a node. defaults to this node
        :param fullpath: return just names, or full paths
        :return: list of attribute names
        """

    @abstractmethod
    def set_node_type(self, new_value: NodeType) -> None:
        """
        Set node_type property for this node
        :param new_value: C (content), CN (content node), AN (abstract node)
        """
