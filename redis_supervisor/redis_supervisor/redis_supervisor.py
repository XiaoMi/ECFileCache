#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
import signal
import logging
import sys

from config import Config
from redis_daemon import RedisDaemon
from redis_perf_monitor import RedisPerfMonitor
from zk_helper import ZkHelper
from utils.log_util import get_logger_name

LOGGER = logging.getLogger(get_logger_name(__name__))

class Supervisor:
    def __init__(self, config):
        zk_client = ZkHelper(config.get_zk_addresses(), int(config.get_zk_timeout()), config.get_zk_root(),
                             config.get_redis_address())

        [host, port] = config.get_redis_address().split(":")
        redis_monitor = RedisPerfMonitor(host, port, config.get_enable_perf_monitor(), config.get_redis_perf_tags())

        self.__redis = RedisDaemon(config, zk_client, redis_monitor)

    def register_signal_handlers(self):
        """
        定义信号的处理函数，以支持meta server的退出控制
        """
        LOGGER.info("will register sigterm handler")
        signal.signal(signal.SIGTERM, self.terminate_handler)
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)

    def terminate_handler(self, signum, frame):
        """
        处理退出事件
        """
        LOGGER.info("terminate event is triggered")
        self.__redis.stop_redis_server(True)

    def run(self):
        self.__redis.start()
        self.__redis.join()

#
# will read config from command line, then start meta server and monitor it
#
if __name__ == "__main__":
    if len(sys.argv) < 3:
        Config.print_usage_help()
        exit(-1)

    Config.init(sys.argv[1:])

    supervisor = Supervisor(Config.instance())
    supervisor.register_signal_handlers()
    supervisor.run()
