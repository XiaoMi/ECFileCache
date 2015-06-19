#!/usr/bin/env python
# -*- coding:utf-8 -*-

import logging
import time

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.recipe import lock
from kazoo.retry import KazooRetry
try:
    from kazoo.handlers.threading import KazooTimeoutError
    TIMEOUT_EXCEPTION = KazooTimeoutError
except ImportError:
    from kazoo.handlers.threading import TimeoutError
    TIMEOUT_EXCEPTION = TimeoutError

from kazoo.exceptions import *
from utils.log_util import get_logger_name, get_lib_logger_name

LOGGER = logging.getLogger(get_logger_name(__name__))
KAZOO_LOGGER = logging.getLogger(get_lib_logger_name(__name__))

class ZkHelperException(Exception):
    def __init__(self, value):
        self.__value = value

    def __str__(self):
        return repr(self.__value)

#
class ZkHelper:
    def __init__(self, zk_addresses, zk_timeout_sec, zk_root, redis_address, redis_id=None):
        """
        :type zk_addresses: str
        :type zk_timeout_sec: int
        :type zk_root: str
        :type redis_address: str
        """
        self.__zk_addresses = zk_addresses  # "zookeeper.n.miliao.com:2181"
        self.__zk_timeout = zk_timeout_sec
        self.__redis_address = bytes(redis_address)  # "192.168.1.1:6379"
        self.__redis_running = False

        self.__zk_registered_redis_id = redis_id
        self.__zk_redis_path = None
        # self.__zk_root = "/xmss/filecache/{clusterid}"
        self.__zk_root = zk_root
        self.__zk_redis_pool = "%s/%s" % (self.__zk_root, "pool")
        self.__zk_lock_path = "%s/%s" % (self.__zk_root, "lock")

        self.__zk_client = None
        self.__zk_lock = None

        self.__stopped = False

        self.init_zk_client()

    def init_zk_client(self):
        zk_retry = KazooRetry(max_tries=20, delay=1.0, ignore_expire=False)
        self.__zk_client = KazooClient(hosts=self.__zk_addresses, logger=KAZOO_LOGGER, timeout=self.__zk_timeout,
                                       connection_retry=zk_retry)
        self.__zk_client.add_listener(self.listener)
        self.__zk_lock = lock.Lock(self.__zk_client, self.__zk_lock_path, identifier=self.__redis_address)

    def list_pool(self):
        self.ensure_path(self.__zk_redis_pool)
        children_str = self.__zk_client.get_children(path=self.__zk_redis_pool)
        children = [int(i) for i in children_str]
        LOGGER.info("current pool:[%s]" % children)
        return children

    def create_redis_node(self):
        try:
            self.__zk_redis_path = "%s/%d" % (self.__zk_redis_pool, self.__zk_registered_redis_id)
            self.__zk_client.create(path=self.__zk_redis_path, value=self.__redis_address, ephemeral=True, makepath=True)
        except (NoNodeError, NoChildrenForEphemeralsError, NodeExistsError, ZookeeperError), e:
            LOGGER.error("zk error when create:%s", self.__zk_redis_path, exc_info=True)
            raise ZkHelperException("zk error when create redis node")

    def register_redis(self):
        try:
            if self.__zk_lock.acquire(blocking=True):
                redis_pool = self.list_pool()
                redis_id = None
                if self.__zk_registered_redis_id is not None:
                    if self.__zk_registered_redis_id not in redis_pool:
                        redis_id = self.__zk_registered_redis_id
                    else:
                        zk_redis_address = self.__zk_client.get(self.__zk_redis_path)
                        if zk_redis_address[0] == self.__redis_address:
                            LOGGER.warn("registered node still available, do nothing. redis id [%d], address [%s]"
                                        % (redis_id, zk_redis_address))
                            return

                if redis_id is None:
                    for redis_id in xrange(len(redis_pool) + 1):
                        if redis_id not in redis_pool:
                            break
                    self.__zk_registered_redis_id = redis_id

                LOGGER.info("register to pool with redis_id:[%d]" % redis_id)
                self.create_redis_node()
                time.sleep(5)
                self.__zk_lock.release()
        except LockTimeout, e:
            verbose = "zk lock acquire timeout"
            LOGGER.error(verbose, exc_info=True)
            raise ZkHelperException(verbose)

    def start(self):
        if self.is_connected():
            return
        try:
            self.__zk_client.start(self.__zk_timeout)
        except TIMEOUT_EXCEPTION, e:
            LOGGER.error("cannot connect to zookeeper", exc_info=True)
            raise ZkHelperException(e)

    def listener(self, stat):
        if stat is None:
            LOGGER.error("get None zk stat")
            return

        if stat == KazooState.CONNECTED:
            LOGGER.info("In stat:" + stat + ". do nothing")
        elif stat == KazooState.SUSPENDED:
            LOGGER.info("In stat:" + stat + ". do nothing")
        elif stat == KazooState.LOST:
            if self.is_redis_running():
                LOGGER.error("In stat:" + stat + ". reconnect")
                self.reconnect()
            else:
                LOGGER.warn("In stat:" + stat + ". redis is not running. Do nothing")
        else:
            LOGGER.warn("unknown status:%s", stat)

    def reconnect(self):

        # wait timeout sec for make sure session expired
        time.sleep(self.__zk_timeout)

        if self.__stopped:
            LOGGER.warn("process has been stopped, do nothing")
            return

        if self.is_connected():
            # if connected, do nothing
            LOGGER.warn("auto reconnected after %d sec, do nothing" % self.__zk_timeout)
            return

        if self.is_redis_running():
            # init and start a new zk_client
            self.init_zk_client()
            self.start()

            self.register_redis()
        else:
            LOGGER.warn("Lost connection to zk, redis is not running. Do nothing")

    def close(self):
        LOGGER.info("stop process and close zk connection")

        self.__stopped = True

        if not self.is_connected():
            return
        # ephemeral node will not be deleted right now, so we delete it manually
        try:
            self.unregister_redis()
            self.__zk_client.stop()
        except ZkHelperException, e:
            LOGGER.error("clean resource failed", exc_info=True)

    def is_connected(self):
        return self.__zk_client.client_state == KazooState.CONNECTED

    def set_redis_running(self, is_running):
        self.__redis_running = is_running

    def is_redis_running(self):
        return self.__redis_running

    def unregister_redis(self):
        if self.__zk_redis_path is None:
            return

        if self.is_connected():
            try:
                self.__zk_client.delete(self.__zk_redis_path)
            except (NoNodeError, ZookeeperError), e:
                LOGGER.warn("node %s doesn`t exists when delete", self.__zk_redis_path, exc_info=True)

    def ensure_path(self, path):
        """
        ensure this path already exists in
        :type path: basestring
        """
        if not self.__zk_client.exists(path):
            LOGGER.warn("path doesn`t exists, path:%s", path)
            self.__zk_client.ensure_path(path)

        return True

    def get_registered_redis_id(self):
        return self.__zk_registered_redis_id

    def get_zk_client(self):
        return self.__zk_client
