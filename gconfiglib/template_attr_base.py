"""Attributes Template - abstract class
"""

import datetime as dt
import json
import sys
from typing import Any, Callable, Optional

from gconfiglib.config_abcs import ConfigAttributeABC
from gconfiglib.enums import Fmt
from gconfiglib.template_base import TemplateBase
from gconfiglib.utils import json_serial


class TemplateAttributeBase(TemplateBase):
    """
    Base attribute template
    """

    def __init__(
        self,
        optional: bool = True,
        value_type: type = str,
        validator: Optional[Callable[[Any], bool]] = None,
        default_value: Optional[Any] = None,
        description: Optional[str] = None,
    ) -> None:
        """
        :param optional: Is this attribute optional (True) or mandatory (False)
        :param value_type: Value type (int, str, etc.)
        :param validator: Validator function. Should take value as argument and return True or False
                            Raises ValueError on any validation failure
        :param default_value: Value to assign if missing from configuration object
        :param description: Attribute description (used when generating sample configuration files)
        """
        self.value_type = value_type
        self.default_value = default_value
        super().__init__(optional, validator, description)

    def validate(
        self, value: ConfigAttributeABC, name: str
    ) -> Optional[ConfigAttributeABC]:
        """
        Validate an attribute
        :param value: Value to be validated
        :param name: Name of the attribute to be validated
        :return: validated value (possibly changed from original), or raises ValueError on failure to validate
        """

        if value.value is not None and not isinstance(value.value, self.value_type):
            try:
                value.value = self.value_type(value.value)
            except ValueError as e:
                if self.value_type == dt.date and isinstance(value.value, dt.datetime):
                    value.value = value.value.date()
                else:
                    if value.value is not None and value.value != "":
                        raise ValueError(
                            f"Expecting {value.get_path()} to be of type {self.value_type}"
                        ) from e

        if self.validator is not None:
            if not self.optional or value.value is not None:
                try:
                    valid = self.validator(value.value)
                    problem = ""
                except ValueError:
                    valid = False
                    problem = sys.exc_info()[0]
                if not valid:
                    message = f"Parameter {value.get_path()} failed validation for value {value.value}"
                    if problem != "":
                        message += f": {problem}"
                    raise ValueError(message)
        if not self.optional and value.value is None:
            # mandatory attribute with no value and no default
            raise ValueError(
                f"Mandatory parameter {value.get_path()} has not been set, and has no default value"
            )

        if value.value is None:
            return None
        return value

    def sample(self, fmt: Fmt = Fmt.JSON) -> str:
        """
        Generate a line for sample configuration file
        :param fmt: JSON or TEXT
        :return: string
        """
        name: str = getattr(self, "name", "Attribute")
        value = self.default_value if self.default_value is not None else ""
        description: str = self.description if self.description is not None else ""
        if fmt == Fmt.JSON:
            return f'"{name}" : {json.dumps(value, ensure_ascii=True, default=json_serial)}'
        if fmt == Fmt.TEXT:
            return f"#\n# {description}\n# {name} = {value}\n"
        return ""
