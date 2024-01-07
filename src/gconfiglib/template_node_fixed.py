""" Fixed Node Template."""
import logging
from typing import Optional

from gconfiglib.config_attribute import ConfigAttribute
from gconfiglib.config_node import ConfigNode
from gconfiglib.template_attr_fixed import TemplateAttributeFixed
from gconfiglib.template_base import TemplateBase
from gconfiglib.template_node_base import TemplateNodeBase
from gconfiglib.template_node_set import TemplateNodeSet

logger = logging.getLogger(__name__)


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

    def validate(self, node: Optional[ConfigNode]) -> Optional[ConfigNode]:
        """
        Validate a node
        :param node: Node to be validated
        :return: validated node (possibly changed from original), or raises ValueError on failure to validate
        """
        logger.debug("Validating node %s", self.name)
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

                new_value: ConfigNode | ConfigAttribute = attr_t.validate(
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
            logger.error(
                "Mandatory node %s is missing, with no defaults set", self.name
            )
            raise ValueError(
                f"Mandatory node {node.get_path()} is missing, with no defaults set"
            )
        if len(node.attributes) == 0:
            return None
        return node
