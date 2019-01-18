# Copyright (C) 2019 Michael Lyubinin
# Author: Michael Lyubinin
# Contact: michael@lyubinin.com

""" Enhanced Configuration library """

import logging

from config import get, set, root, init, TemplateAttributeFixed, TemplateAttributeVariable, \
    TemplateNodeFixed, TemplateNodeVariableAttr, TemplateNodeSet, ConfigNode, ConfigAttribute

logging.getLogger('gconfiglib').addHandler(logging.NullHandler())

