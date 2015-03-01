# -*- coding: utf-8 -*-
"""
Unit tests for configuration stuff
"""
from __future__ import absolute_import
from __future__ import print_function

import testify

import vimap.config


class TimeoutConfigTest(testify.TestCase):
    def test_default_config_and_properties(self):
        cfg = vimap.config.TimeoutConfig.default_config()
        testify.assert_equal(cfg.general_timeout, 5.0)
        testify.assert_equal(cfg.input_queue_put_timeout, 5.0)
