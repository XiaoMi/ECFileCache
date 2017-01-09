#!/usr/bin/env python
# -*- coding:utf-8 -*-

import logging
import time

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.recipe import lock
from kazoo.retry import KazooRetry
from kazoo.exceptions import *
from utils.log_util import get_logger_name, get_lib_logger_name

try:
  from kazoo.handlers.threading import KazooTimeoutError

  TIMEOUT_EXCEPTION = KazooTimeoutError
except ImportError:
  from kazoo.handlers.threading import TimeoutError

  TIMEOUT_EXCEPTION = TimeoutError

LOGGER = logging.getLogger(get_logger_name(__name__))
KAZOO_LOGGER = logging.getLogger(get_lib_logger_name(__name__))


class ZkHelperException(Exception):
  def __init__(self, value):
    self.__value = value

  def __str__(self):
    return repr(self.__value)


REGISTER_WORKER_FREEZE_TIME = 3 * 2  # REDIS_SERVER_START_FREEZE_TIME * 2
REGISTER_WORKER_WAIT_REGISTER_SEC = 10
REGISTER_WORKER_WAIT_CONNECT_SEC = 1
REGISTER_NODE_RETRY_WAIT_TIME_SEC = 40  #
REGISTER_NODE_MAX_RETRY = 3  #

class ZkHelper:
  def __init__(self, zk_addresses, zk_timeout_sec, zk_root, redis_address, redis_id=None):
    """ create zookeeper client, keep connection and auto re-register when session expired
    :type zk_addresses: str
    :type zk_timeout_sec: int
    :type zk_root: str
    :type redis_address: str
    """
    self.__zk_addresses = zk_addresses  # "zookeeper.n.miliao.com:2181"
    self.__zk_timeout = zk_timeout_sec
    self.__redis_address = bytes(redis_address)  # "192.168.1.1:6379"

    self.__zk_registered_redis_id = redis_id
    self.__zk_redis_path = None
    self.__zk_root = zk_root  # "/xmss/cluster/{clusterid}/filecache/partition_{id}"
    self.__zk_redis_pool = "%s/%s" % (self.__zk_root, "pool")
    self.__zk_lock_path = "%s/%s" % (self.__zk_root, "lock")

    # init zookeeper client
    zk_retry = KazooRetry(max_tries=3, delay=1.0, ignore_expire=False)  # delay in sec
    self.__zk_client = KazooClient(hosts=self.__zk_addresses, logger=KAZOO_LOGGER, timeout=self.__zk_timeout,
                     connection_retry=zk_retry)
    self.__zk_client.add_listener(self.listener)
    self.__zk_lock = lock.Lock(self.__zk_client, self.__zk_lock_path, identifier=self.__redis_address)

    # auto re-register worker
    self.__lost_zk_connection = False
    self.__re_register_event = self.__zk_client.handler.event_object()
    self.__re_register_thread = None

    self.__supervisor = None
    self.__stopped = False

  def list_pool(self):
    try:
      if not self.__zk_client.exists(self.__zk_redis_pool):
        LOGGER.warn("zk path[%s] doesn`t exists. create it.", self.__zk_redis_pool)
        self.__zk_client.ensure_path(self.__zk_redis_pool)

      children_str = self.__zk_client.get_children(path=self.__zk_redis_pool)
      children = [int(i) for i in children_str]
      LOGGER.info("current pool:[%s]", children)
      return children
    except (ConnectionLoss, ZookeeperError), e:
      LOGGER.error("zk exception when list pool.")
      raise ZkHelperException(e)

  def create_redis_node(self):
    try:
      self.__zk_redis_path = "%s/%d" % (self.__zk_redis_pool, self.__zk_registered_redis_id)
      self.__zk_client.create(path=self.__zk_redis_path, value=self.__redis_address, ephemeral=True, makepath=True)
    except (NoNodeError, NoChildrenForEphemeralsError, NodeExistsError, ConnectionLoss, ZookeeperError), e:
      LOGGER.error("zk exception when create redis node.")
      raise ZkHelperException(e)

  def check_alreay_registered(self, redis_pool):
    """
    :param redis_pool: list
    :return need register redis address or not
    :rtype boolean
    """
    already_registered = False
    if self.__zk_registered_redis_id is not None and self.__zk_registered_redis_id in redis_pool:
      zk_redis_address = self.__zk_client.get(self.__zk_redis_path)
      if zk_redis_address[0] == self.__redis_address:
        LOGGER.warn("redis address registered. redis id [%d], address [%s]",
              self.__zk_registered_redis_id, zk_redis_address)
        already_registered = True
      else:
        # registered id expired, and re-registered by another redis address
        self.__zk_registered_redis_id = None

    return already_registered

  def check_and_get_valid_register_id(self, redis_pool):
    """
    :param redis_pool:  list
    :return: register id : string
    :raise ZkHelperException:
    """
    if self.__zk_registered_redis_id is not None:
      LOGGER.warn("registered redis id[%s] still valid, register it" % self.__zk_registered_redis_id)
      return self.__zk_registered_redis_id

    redis_address_registered = False
    for i in xrange(REGISTER_NODE_MAX_RETRY):
      redis_address_registered = False
      for redis_node in redis_pool:
        zk_redis_path = "%s/%d" % (self.__zk_redis_pool, redis_node)
        zk_redis_address = self.__zk_client.get(zk_redis_path)
        if zk_redis_address[0] == self.__redis_address:
          LOGGER.warn("redis address already in zk, sleep [%d]s. redis zk id [%d], address [%s]. retry [%d] times.",
                REGISTER_NODE_RETRY_WAIT_TIME_SEC, redis_node, self.__redis_address, i)

          redis_address_registered = True
          time.sleep(REGISTER_NODE_RETRY_WAIT_TIME_SEC * (i + 1))
          redis_pool = self.list_pool()
          break
      if not redis_address_registered:
        break

    if redis_address_registered:
      raise ZkHelperException("redis address already exist in zookeeper. EXIT after retry [%d] times" % REGISTER_NODE_MAX_RETRY)

    redis_id = None
    for redis_id in xrange(len(redis_pool) + 1):
      if redis_id not in redis_pool:
        break
    self.__zk_registered_redis_id = redis_id

    return self.__zk_registered_redis_id

  def register_redis(self):
    try:
      LOGGER.debug("acquiring lock")
      if self.__zk_lock.acquire(blocking=True):
        try:
          LOGGER.debug("lock acquired")
          redis_pool = self.list_pool()
          if self.check_alreay_registered(redis_pool):
            return

          self.check_and_get_valid_register_id(redis_pool)
          self.create_redis_node()
          LOGGER.warn("register to pool with redis_id:[%d]", self.__zk_registered_redis_id)
        finally:
          self.__zk_lock.release()
          LOGGER.debug("release lock")

    except LockTimeout, e:
      LOGGER.error("acquire lock timeout.")
      raise ZkHelperException(e)
    except ConnectionClosedError, e:
      LOGGER.error("zk connection closed when register redis.")
      raise ZkHelperException(e)

  def start(self):
    if self.is_connected():
      return
    try:
      self.__zk_client.start(self.__zk_timeout)
    except TIMEOUT_EXCEPTION, e:
      LOGGER.error("cannot connect to zookeeper.")
      raise ZkHelperException(e)

  def listener(self, stat):
    if stat is None:
      LOGGER.error("get `None` zk stat")
      return

    if stat == KazooState.CONNECTED:
      if self.is_redis_running():
        if self.__lost_zk_connection:
          LOGGER.info("In stat:" + stat + ". set re-register flag")
          self.__re_register_event.set()
          self.__lost_zk_connection = False
        else:
          LOGGER.info("In stat:" + stat + ". did not lost connection, do nothing")
      else:
        LOGGER.debug("In stat:" + stat + ". redis is not running. Do nothing")
    elif stat == KazooState.SUSPENDED:
      LOGGER.info("In stat:" + stat + ". do nothing")
    elif stat == KazooState.LOST:
      LOGGER.info("In stat:" + stat + ". set lost flag")
      self.__lost_zk_connection = True
    else:
      LOGGER.warn("unknown status:[%s]", stat)

  def is_zk_connection_stopped(self):
    return self.__zk_client._connection.connection_stopped.is_set()

  def register_worker(self):
    time.sleep(REGISTER_WORKER_FREEZE_TIME)

    while not self.__stopped:
      LOGGER.debug("Register worker...")

      if not self.is_redis_running():
        LOGGER.warn("redis is not running, stop all.")
        break
      elif self.is_zk_connection_stopped():
        LOGGER.warn("zk connection stopped, stop all.")
        self.__supervisor.stop_all(True)
        break

      self.__re_register_event.wait(REGISTER_WORKER_WAIT_REGISTER_SEC)
      if self.__re_register_event.is_set():
        try:
          if not self.is_connected():
            # zk not connected, do nothing
            LOGGER.warn("Not connected, do nothing")
            time.sleep(REGISTER_WORKER_WAIT_CONNECT_SEC)
            continue

          LOGGER.warn("Re register redis")
          self.register_redis()

          # clear re_register_event if register success
          self.__re_register_event.clear()
        except ZkHelperException, e:
          LOGGER.warn("re-register to zk exception:[%s]", str(e))

    LOGGER.warn("exit register worker")

  def stop(self):
    LOGGER.info("stop process and close zk connection")

    if self.__stopped:
      return

    if not self.is_connected():
      return

    # ephemeral node will not be deleted right now, so we delete it manually
    self.unregister_redis()

    self.__zk_client.stop()

    if self.__re_register_thread:
      self.__re_register_event.set()
      self.__re_register_thread.join()
      self.__re_register_thread = None

    self.__stopped = True

  def is_connected(self):
    return self.__zk_client.client_state == KazooState.CONNECTED

  def is_redis_running(self):
    return self.__supervisor is not None

  def set_supervisor(self, supervisor):
    self.__supervisor = supervisor
    self.__re_register_thread = self.__zk_client.handler.spawn(self.register_worker)

  def unregister_redis(self):
    if self.__zk_redis_path is None:
      return

    if self.is_connected():
      try:
        redis_pool = self.list_pool()
        if self.check_alreay_registered(redis_pool):
          LOGGER.warn("delete id [%d], address [%s]", self.__zk_registered_redis_id, self.__redis_address)
          self.__zk_client.delete(self.__zk_redis_path)
      except (NoNodeError, ZookeeperError), e:
        LOGGER.info("node [%s] doesn`t exists when delete", self.__zk_redis_path)

  def get_registered_redis_id(self):
    return self.__zk_registered_redis_id

  # for test
  def get_zk_client(self):
    return self.__zk_client
