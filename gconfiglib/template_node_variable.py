""" Node with Variable Attributes Template."""
from collections import OrderedDict
from typing import Any, Callable, Optional

from gconfiglib.config_attribute import ConfigAttribute
from gconfiglib.config_node import ConfigNode
from gconfiglib.enums import NodeType
from gconfiglib.template_attr_variable import TemplateAttributeVariable
from gconfiglib.template_node_base import TemplateNodeBase


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
        node_type: Optional[NodeType] = None,
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

    def validate(self, node: Optional[ConfigNode]) -> Optional[ConfigNode]:
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
