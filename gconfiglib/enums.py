""" Enums used for gconfiglib"""

from enum import Enum, auto


class NodeType(Enum):
    """Node Types - used for config storage in ZooKeeper
    AN - abstract node. Can contain only other nodes
    CN - content node. Can contain both nodes and attributes
    C - content. This node type stores its contents not as child nodes, but directly
    as json object.
    """

    AN = auto()
    CN = auto()
    C = auto()


class Fmt(Enum):
    """Format of configuration files"""

    JSON = auto()
    TEXT = auto()
