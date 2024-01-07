"""TemplateBase class - common elements for all template objects
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Optional

from gconfiglib.config_abcs import ConfigObject
from gconfiglib.enums import Fmt


class TemplateBase(ABC):
    """
    Common elements for all template objects
    """

    def __init__(
        self,
        optional: bool = True,
        validator: Optional[Callable[[Any], bool]] = None,
        description: Optional[str] = None,
    ) -> None:
        self.optional = optional
        self.validator = validator
        self.description = description

    @abstractmethod
    def sample(self, fmt: Fmt = Fmt.JSON) -> str:
        """Generate sample configuration

        Args:
            fmt (str, optional): JSON or TEXT. Defaults to "JSON".

        Returns:
            str: Sample configuration as string
        """

    @abstractmethod
    def validate(self, value: Optional[ConfigObject]) -> Optional[ConfigObject]:
        """
        Validate an attribute or a node
        :param value: Value to be validated
        :return: validated value (possibly changed from original)
        """
