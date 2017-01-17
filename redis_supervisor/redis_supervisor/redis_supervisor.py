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

#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
import sys
import os
import signal
import logging
import time
import subprocess

from utils.config import Config
from zk_helper import ZkHelperException
from utils.log_util import get_logger_name
from zk_helper import ZkHelper

LOGGER = logging.getLogger(get_logger_name(__name__))

REDIS_SERVER_START_FREEZE_TIME = 3
REDIS_SERVER_STOP_FREEZE_TIME = 3


class Supervisor:
  def __init__(self, config):
    self.__zk_client = ZkHelper(config.get_zk_addresses(), int(config.get_zk_timeout()), config.get_zk_root(),
                  config.get_redis_address())

    self.__redis_bin = config.get_redis_bin()
    self.__redis_conf = config.get_redis_conf()
    self.__redis_address = config.get_redis_address()

    self.__redis_process = None
    self.__stopped = False

  def register_signal_handlers(self):
    """ define signal processor
    """
    LOGGER.info("register sigterm handler")
    signal.signal(signal.SIGTERM, self.terminate_handler)
    signal.signal(signal.SIGINT, self.terminate_handler)
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)

  def terminate_handler(self, signum, frame):
    """ process terminate signal
    """
    LOGGER.info("terminate event is triggered")
    self.stop_all(True)

  def run(self):
    """ run redis server, and register server address to zk
    """
    self.__zk_client.start()

    if not self.__stopped:
      LOGGER.info("try to start redis server")

      # start redis server and keep it running
      if not os.path.exists("logs"):
        os.mkdir("logs")
      out_log_name = "logs/redis-out-%s.log" % self.__redis_address
      out_log = open(out_log_name, "a")
      err_log = open("logs/redis-err-%s.log" % self.__redis_address, "a")

      commands = [self.__redis_bin, self.__redis_conf]
      self.__redis_process = subprocess.Popen(commands, bufsize=1024 * 1024, stdout=out_log, stderr=err_log)

      time.sleep(REDIS_SERVER_START_FREEZE_TIME)

      out_log_check = open(out_log_name, "r")
      last_line = ""
      for line in out_log_check:
        last_line = line

      if last_line.find("The server is now ready to accept connections on port") > 0:
        LOGGER.info("start redis server succeed, pid:%d", self.__redis_process.pid)
        self.__zk_client.set_supervisor(self)
        self.__zk_client.register_redis()
      else:
        LOGGER.error("start redis server failed, pid:%d", self.__redis_process.pid)

      ret_code = self.__redis_process.wait()
      self.__redis_process = None
      LOGGER.warn("redis server stopped, code:%d", ret_code)

      self.stop_all()

  def stop_all(self, blocking=False):
    """ set stop falg, send terminate signal to redis server
    """
    if self.__stopped:
      return

    self.__stopped = True

    self.__zk_client.stop()

    if self.__redis_process:
      try:
        LOGGER.info("try to stop redis server")
        self.__redis_process.send_signal(signal.SIGTERM)

        if blocking:
          # wait until redis process return
          LOGGER.info("wait until redis server quit")
          time.sleep(REDIS_SERVER_STOP_FREEZE_TIME)

          ret_code = self.__redis_process.poll()
          LOGGER.info("redis server exit with code:%d", ret_code)

          if ret_code is None:
            LOGGER.warning("redis server of config[%s] do not exit in time", self.__redis_conf)

            # kill redis process
            self.__redis_process.kill()
            LOGGER.warning("killed redis server")

          self.__redis_process = None

      except OSError, e:
        LOGGER.error("redis server already exit. %s", str(e))



#
# will read config from command line, then start redis and monitor it
#
if __name__ == "__main__":
  if len(sys.argv) < 3:
    Config.print_usage_help()
    exit(-1)

  Config.init(sys.argv[1:])

  supervisor = Supervisor(Config.instance())
  supervisor.register_signal_handlers()

  try:
    supervisor.run()
  except ZkHelperException, e:
    LOGGER.exception("zk helper exception:")

  supervisor.stop_all(True)

  LOGGER.warn("Exit. Bye bye")
