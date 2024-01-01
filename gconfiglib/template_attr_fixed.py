""" Fixed Attribute Template."""
from typing import Any, Callable, Optional

from gconfiglib.config_attribute import ConfigAttribute
from gconfiglib.template_attr_base import TemplateAttributeBase


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

    def validate(self, value: Optional[ConfigAttribute]) -> Optional[ConfigAttribute]:
        """
        Validate an attribute
        :param value: Value to be validated
        :return: validated value (possibly changed from original), or raises ValueError on failure to validate
        """
        if not value:
            value = ConfigAttribute(self.name, self.default_value)
        return super().validate(value, self.name)
