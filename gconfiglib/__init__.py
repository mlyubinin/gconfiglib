# Copyright (C) 2019 Michael Lyubinin
# Author: Michael Lyubinin
# Contact: michael@lyubinin.com

""" Enhanced Configuration library. """

# ruff: noqa: F401
# flake8: noqa: F401


import logging

from .config import (
    ConfigAttribute,
    ConfigNode,
    TemplateAttributeFixed,
    TemplateAttributeVariable,
    TemplateNodeFixed,
    TemplateNodeSet,
    TemplateNodeVariableAttr,
    get,
    init,
    root,
    set,
)

logging.getLogger("gconfiglib").addHandler(logging.NullHandler())
