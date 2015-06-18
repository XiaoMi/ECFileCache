#!/usr/bin/env python
# -*- coding:utf-8 -*-
#

import signal
import logging
import time
import subprocess
from threading import Thread
from utils.log_util import get_logger_name

from utils.atomic import AtomicBoolean
from zk_helper import ZkHelper
from utils.xbox_util import WarningUtil

LOGGER = logging.getLogger(get_logger_name(__name__))

class RedisDaemon(Thread):
    REDIS_SERVER_START_FREEZE_TIME = 10
    REDIS_SERVER_STOP_FREEZE_TIME = 20

    def __init__(self, config, zk_client, redis_monitor):
        """

        :type config: PpConfig
        :type zk_client: ZkHelper
        """

        Thread.__init__(self)

        self.__redis_bin = config.get_redis_bin()
        self.__redis_conf = config.get_redis_conf()
        self.__redis_address = config.get_redis_address()
        self.__zk_client = zk_client
        self.__redis_monitor = redis_monitor

        self.__redis_process = None
        self.__stop_acquired = AtomicBoolean()

    def get_redis_bin(self):
        """
        :rtype: str
        """
        return self.__redis_bin

    def get_redis_conf(self):
        """
        :rtype: str
        """
        return self.__redis_conf

    def is_stop_acquired(self):
        """
        :rtype: bool
        """
        return self.__stop_acquired.getBoolean()

    def run(self):
        """
        rewrite run function, this function will start redis server and keep it running
        no matter redis server exit normally or exist for exception.
        only when terminate signal is sent to this app, it will stop this redis server
        """
        self.__zk_client.start()
        while not self.is_stop_acquired():

            LOGGER.info("try to start redis server")

            # start the sub process and keep it running
            out_log_name = "logs/redis-out_%s.log" % self.__redis_address
            out_log = open(out_log_name, "a")
            err_log = open("logs/redis-err_%s.log" % self.__redis_address, "a")

            commands = [self.__redis_bin, self.__redis_conf]
            self.__redis_process = subprocess.Popen(commands, bufsize=1024 * 1024, stdout=out_log, stderr=err_log)

            time.sleep(RedisDaemon.REDIS_SERVER_START_FREEZE_TIME)

            out_log_check = open(out_log_name, "r")
            last_line = ""
            for line in out_log_check:
                last_line = line

            if last_line.find("The server is now ready to accept connections on port") > 0:
                LOGGER.info("start redis server succeed, pid:%d", self.__redis_process.pid)
                self.__zk_client.set_redis_running(True)
                self.__zk_client.register_redis()
                self.__redis_monitor.start()
            else:
                self.__zk_client.set_redis_running(False)
                LOGGER.error("start redis server fail, pid:%d", self.__redis_process.pid)

            ret_code = self.__redis_process.wait()
            LOGGER.info("redis server closed, code:%d", ret_code)

            if not self.is_stop_acquired():
                self.__zk_client.unregister_redis()

        self.__zk_client.close()
        self.__redis_monitor.stop()

    def stop_redis_server(self, blocking=False):
        """
        设置停止位，并向redis server进程发送停止信号
        """
        self.__zk_client.close()

        if self.__redis_process:
            try:
                LOGGER.warn("try to stop redis server")
                self.__stop_acquired.setBoolean(True)
                self.__redis_process.send_signal(signal.SIGTERM)

                if blocking:
                    # 等待直到完全退出
                    LOGGER.info("try to wait until redis server quit")

                    time.sleep(RedisDaemon.REDIS_SERVER_STOP_FREEZE_TIME)
                    ret_code = self.__redis_process.poll()

                    if ret_code is None:
                        error_message = "redis server did not exit in time, config content:%s" % self.get_redis_conf()
                        LOGGER.error(error_message)
                        WarningUtil.sendError(error_message)

                        # 直接kill相应的进程
                        self.__redis_process.kill()
                        LOGGER.warning("redis server has been killed")

                    LOGGER.info("redis server has exit safely, code:%d", ret_code)
            except OSError, e:
                LOGGER.error("redis server process already exit", exc_info = True)
        else:
            LOGGER.info("redis server is not alive, nothing to do")
