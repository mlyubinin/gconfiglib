""" Configuration attribute class."""
from typing import Any, Optional

from gconfiglib.config_abcs import ConfigAttributeABC, ConfigNodeABC


class ConfigAttribute(ConfigAttributeABC):
    """
    Configuration attribute class
    """

    def __init__(
        self, name: str, value: Any, parent: Optional[ConfigNodeABC] = None
    ) -> None:
        self.name: str = name
        self.value: Any = value
        self.parent: Optional[ConfigNodeABC] = parent

    def _set_parent(self, parent_node: ConfigNodeABC) -> None:
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

    def __str__(self) -> str:
        """
        Get formatted node content
        """
        indent = "\t"
        depth: int = 0
        if self.parent:
            depth = self.parent.depth
        return f"{indent * depth}{self.name} = {self.value} ({type(self.value)})"

    def __repr__(self) -> str:
        """Get string representation of this attribute

        Returns:
            str: string representation of attribute's content
        """
        return self.value.__repr__()
