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
import logging
import threading
import time
import unittest
from kazoo.client import KazooClient
from kazoo.retry import KazooRetry
from redis_supervisor.utils.log_util import get_logger_name, get_lib_logger_name
from redis_supervisor.zk_helper import ZkHelper

ZK_ADDRESSES = "zk1.srv:2222,zk2.srv:2222"
ZK_TIMEOUT = 4  # zk session timeout, from 4 to 40 sec
ZK_ROOT = "/filecache/cluster"
REDIS_ADDRESS = "127.0.0.1:63300"

LOGGER = logging.getLogger(get_logger_name(__name__))
KAZOO_LOGGER = logging.getLogger(get_lib_logger_name(__name__))


class MockSupervisor:
  def __init__(self):
    pass

  def stop_all(self, arg):
    pass


class ZkHelperTest(unittest.TestCase):
  def test_concurrent_startup(self):
    class ZkHelperThread(threading.Thread):
      def __init__(self):
        super(ZkHelperThread, self).__init__()
        self.zk_helper = ZkHelper(ZK_ADDRESSES, ZK_TIMEOUT, ZK_ROOT, REDIS_ADDRESS)

      def run(self):
        self.zk_helper.start()
        self.zk_helper.register_redis()

      def stop(self):
        self.zk_helper.stop()

    LOGGER.warn("test concurrent startup")
    zk_clients = []
    expect = []
    for i in xrange(0, 10):
      zk_clients.append(ZkHelperThread())
      expect.append(i)

    for i in xrange(0, 10):
      zk_clients[i].start()

    #time.sleep(60)
    for i in xrange(0, 10):
      zk_clients[i].join()

    zk_children = zk_clients[0].zk_helper.get_zk_client().get_children(path="%s/pool" % ZK_ROOT)
    zk_children = [int(i) for i in zk_children]
    zk_children.sort()
    self.assertListEqual(expect, zk_children)

    for i in xrange(0, 10):
      zk_clients[i].stop()

  def test_timeout(self):
    LOGGER.warn("test timeout")
    self.runZkHelper()
    #pass

  def runZkHelper(self):
    zk_helper = ZkHelper(ZK_ADDRESSES, ZK_TIMEOUT, ZK_ROOT, REDIS_ADDRESS)

    zk_helper.start()
    zk_helper.register_redis()
    zk_helper.set_supervisor(MockSupervisor())
    # cmd: sudo tc qdisc add dev eth0 root netem delay 4s
    # cmd: sudo tc qdisc delete dev eth0 root netem delay 4s

    time.sleep(600)

    zk_helper.stop()

  def test_reconnect_when_zk_session_expire(self):
    LOGGER.warn("test zk_session_expire with redis running")

    self.reconnect_when_zk_session_expire(MockSupervisor())

    time.sleep(10)

    LOGGER.warn("test zk_session_expire with redis stop")
    self.reconnect_when_zk_session_expire(None)

  def reconnect_when_zk_session_expire(self, supervisor):
    zk_helper = ZkHelper(ZK_ADDRESSES, ZK_TIMEOUT, ZK_ROOT, REDIS_ADDRESS)

    zk_helper.start()
    zk_helper.register_redis()

    # suppose redis already started
    zk_helper.set_supervisor(supervisor)

    # make zk session expired
    zk_client = zk_helper.get_zk_client()
    client_id = zk_client.client_id
    time.sleep(10)

    zk_retry = KazooRetry(max_tries=3, delay=1.0, ignore_expire=False)
    zk_client_new = KazooClient(hosts=ZK_ADDRESSES, timeout=ZK_TIMEOUT, connection_retry=zk_retry,
                  client_id=client_id, logger=KAZOO_LOGGER)
    zk_client_new.start(ZK_TIMEOUT)
    zk_client_new.stop()
    # zk session expired done

    time.sleep(ZK_TIMEOUT)

    zk_children = zk_helper.get_zk_client().get_children(path="%s/pool" % ZK_ROOT)
    zk_children = [int(i) for i in zk_children]
    if supervisor:
      self.assertTrue(zk_helper.get_registered_redis_id() in zk_children)
    else:
      self.assertFalse(zk_helper.get_registered_redis_id() in zk_children)

    zk_helper.stop()

if __name__ == "__main__":
  unittest.main()

  '''
  # to run a single test
  suite = unittest.TestSuite()
  suite.addTest(ZkHelperTest("test_timeout"))
  #suite.addTest(ZkHelperTest("test_concurrent_startup"))
  #suite.addTest(ZkHelperTest("test_reconnect_when_zk_session_expire"))
  runner = unittest.TextTestRunner()
  runner.run(suite)
  '''
