""" Variable Attribute Template."""
from typing import Any, Callable, Optional

from gconfiglib.template_attr_base import TemplateAttributeBase


class TemplateAttributeVariable(TemplateAttributeBase):
    """
    Variable configuration attribute template class
    For an attribute with a name not known until runtime
    """

    def __init__(
        self,
        value_type: type = str,
        validator: Optional[Callable[[Any], bool]] = None,
        description: Optional[str] = None,
    ) -> None:
        """
        :param value_type: Value type (int, str, etc.)
        :param validator: Validator function. Should take value as argument and return new, possibly changed value
                            Raises ValueError on any validation failure
        :param description: Attribute description (used when generating sample configuration files)
        """
        super().__init__(True, value_type, validator, description=description)
