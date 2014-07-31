# -*- coding: utf-8 -*-
"""
New abstractions for pool configuration. This file will eventually expose more
settings as we see fit.
"""
from __future__ import absolute_import
from __future__ import print_function

from collections import namedtuple


class TimeoutConfig(namedtuple("TimeoutConfig", ["general_timeout"])):
    @property
    def input_queue_put_timeout(self):
        return self.general_timeout
