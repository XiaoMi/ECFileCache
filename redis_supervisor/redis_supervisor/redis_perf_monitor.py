#!/usr/bin/env python
# -*- coding:utf-8 -*-
import json
import logging
import requests
from requests import ConnectionError
import redis
import time
from threading import Thread
from config import Config
from utils.log_util import get_logger_name

SOCKET_TIMEOUT = 20  # sec
SOCKET_CONNECT_TIMEOUT = 20.0  # sec
MONITOR_STEP = 10
PUSH_URL = "http://127.0.0.1:1988/v1/push"

LOGGER = logging.getLogger(get_logger_name(__name__))

class RedisPerfMonitor(Thread):

    @classmethod
    def create(cls):
        return cls()

    def __init__(self, host, port, is_enabled, tags):
        Thread.__init__(self)
        self.__redis_client = redis.StrictRedis(host=host,
                                                port=port,
                                                socket_timeout=SOCKET_TIMEOUT,
                                                socket_connect_timeout=SOCKET_CONNECT_TIMEOUT,
                                                socket_keepalive=True)
        self.__host = host
        self.__port = port
        self.__is_enabled = is_enabled
        self.__tags = tags  # "idc=lg,loc=beijing"

        self.__stop = False

    def run(self):
        if not self.__is_enabled:
            return

        while not self.__stop:
            try:
                redis_info = self.__redis_client.info('stats')
                keyspace_misses = redis_info['keyspace_misses']
                evicted_keys = redis_info['evicted_keys']
            except BaseException, e:
                keyspace_misses = -1
                evicted_keys = -1
                LOGGER.error("access redis exception", exc_info=True)

            self.post_perf_info(self.make_perf_info(keyspace_misses, evicted_keys))

            time.sleep(MONITOR_STEP)

    def make_perf_info(self, key_misses, evicted_keys):
        ts = int(time.time())
        payload = [
            {
                "endpoint": self.__host,
                "metric": "filecache.redis.key_misses",
                "timestamp": ts,
                "step": MONITOR_STEP,
                "value": key_misses,
                "counterType": "GAUGE",
                "tags": self.__tags,
            },
            {
                "endpoint": self.__host,
                "metric": "filecache.redis.evicted_keys",
                "timestamp": ts,
                "step": MONITOR_STEP,
                "value": evicted_keys,
                "counterType": "GAUGE",
                "tags": self.__tags,
            },
        ]

        return json.dumps(payload)

    def post_perf_info(self, perf_info):
        try:
            requests.post(PUSH_URL, data=perf_info)
        except ConnectionError, e:
            LOGGER.error("access perf agent exception", exc_info=True)

    def stop(self):
        self.__stop = True
