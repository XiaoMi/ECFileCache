# Copyright 2016 Xiaomi, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/python
# -*- coding: utf-8 -*-
#

import logging
import logging.config
import os

LOGGER_ROOT = "app"
LOGGER_LIB_ROOT = "lib"
LOGGER_TEST_ROOT = "test"

LOGGER_CONF_FILE = "conf/log4p.conf"
logging.config.fileConfig(LOGGER_CONF_FILE)

def get_logger_name(name):
  """
  get logger name
  """
  LOG_NAME = "%s.%s" % (LOGGER_ROOT, name)
  return LOG_NAME

def get_lib_logger_name(name):
  """
  get logger name of lib
  """
  LOG_NAME = "%s.%s" % (LOGGER_LIB_ROOT, name)
  return LOG_NAME

def get_test_logger_name(name):
  """
  get logger name  of test lib
  """
  LOG_NAME = "%s.%s" % (LOGGER_TEST_ROOT, name)
  return LOG_NAME

