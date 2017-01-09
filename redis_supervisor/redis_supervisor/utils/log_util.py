#
# -*- coding: utf-8 -*-
#
#
#

__author__ = 'sail'

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

