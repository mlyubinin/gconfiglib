""" Node Set Template."""
import logging
from typing import List, Optional

from gconfiglib.config_attribute import ConfigAttribute
from gconfiglib.config_node import ConfigNode
from gconfiglib.enums import Fmt
from gconfiglib.template_node_base import TemplateNodeBase

logger = logging.getLogger(__name__)


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

    def validate(self, node: Optional[ConfigNode]) -> Optional[ConfigNode]:
        """
        Validate a node
        :param node: Node to be validated
        :return: validated node (possibly changed from original), or raises ValueError on failure to validate
        """

        logger.debug("Validating nodeset %s", self.name)
        node = super().validate(node)
        # If None, pass it back without further checks (missing optional node was not created)
        if node is None:
            return None
        for name in self.names_lst:
            if name not in node.attributes.keys():
                if not self.attributes["node"].optional:
                    node.add(ConfigNode(name))
                    logger.error(
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

    def sample(self, fmt: Fmt = Fmt.JSON) -> str:
        """
        Generate a line for sample configuration file
        :param fmt: JSON or TEXT
        :return: string
        """
        description: str = (
            self.attributes["node"].description
            if self.attributes["node"].description
            else ""
        )
        result: str
        for node_name in self.names_lst:
            if fmt == Fmt.JSON:
                result = '"%s" : {' % node_name
                for attribute in self.attributes["node"].attributes.values():
                    result += attribute.sample(fmt) + ", "
                result = result[:-2] + "}, "
            elif fmt == Fmt.TEXT:
                result = f"# {description}\n# [{node_name}]\n"
                for attribute in self.attributes["node"].attributes.values():
                    if isinstance(attribute, TemplateNodeBase):
                        raise ValueError(
                            "Text format configuration files are not supported for multi-level node hierarchy"
                        )
                    result += attribute.sample(fmt)
            else:
                raise ValueError("Unsupported sample format")
        if fmt == Fmt.JSON:
            result = result[:-2]
        return result
