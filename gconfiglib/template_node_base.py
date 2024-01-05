""" Base Node Template."""
import logging
from collections import OrderedDict
from typing import Any, Callable, Optional

from gconfiglib.config_node import ConfigNode
from gconfiglib.enums import Fmt, NodeType
from gconfiglib.template_base import TemplateBase

logger = logging.getLogger(__name__)


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
        node_type: Optional[NodeType] = None,
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
        self.node_type = node_type

        super().__init__(optional, validator, description)

    def validate(self, node: Optional[ConfigNode]) -> Optional[ConfigNode]:
        """
        Validate a node
        :param node: Node to be validated
        :return: validated node (possibly changed from original), or raises ValueError on failure to validate
        """
        if node is None:
            # Empty node - try creating a node if it's mandatory, otherwise return None
            if self.optional:
                logger.debug("Node %s is missing, but it's optional", self.name)
                return None
            logger.debug(
                "Mandatory node %s is missing, creating one from default values",
                self.name,
            )
            node = ConfigNode(self.name)
        elif not isinstance(node, ConfigNode):
            logger.debug(
                "Configuration object passed for validation to template %s is not a ConfigNode",
                self.name,
            )
            raise ValueError(
                f"Configuration object passed for validation to template {self.name} is not a ConfigNode"
            )
        elif len(self.attributes) == 0:
            logger.debug("Template for node %s has no attributes", self.name)
            raise ValueError(f"Template for node {self.name} has no attributes")
        # TODO self.validator needs to be run after attributes' validator functions have been run
        if self.validator is not None:
            try:
                valid = self.validator(node.get())
                problem = ""
            except ValueError as e:
                valid = False
                problem = str(e)
            if not valid:
                message = f"Node {node.get_path()} failed validation"
                if problem != "":
                    message += f": {problem}"
                logger.error(message)
                raise ValueError(message)
        if self.node_type and self.node_type != node.node_type:
            node.set_node_type(self.node_type)
        return node

    def sample(self, fmt: Fmt = Fmt.JSON) -> str:
        """
        Generate a line for sample configuration file
        :param fmt: JSON or TEXT
        :return: string
        """
        description: str = self.description if self.description else ""
        if fmt == Fmt.JSON:
            if self.name == "root":
                result: str = "{"
            else:
                result = '"%s" : {' % self.name
            for attribute in self.attributes.values():
                result += attribute.sample(fmt) + ", "
            result = result[:-2] + "}"
        elif fmt == Fmt.TEXT:
            if self.name == "root":
                result = ""
            else:
                result = f"# {description}\n# [{self.name}]\n"
            for attribute in self.attributes.values():
                if self.name != "root" and isinstance(attribute, TemplateNodeBase):
                    raise ValueError(
                        "Text format configuration files are not supported for multi-level node hierarchy"
                    )
                result += attribute.sample(fmt)
        else:
            raise ValueError("Unsupported sample format")
        return result
