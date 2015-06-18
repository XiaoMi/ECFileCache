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

ZK_ADDRESSES = "zookeeper.n.miliao.com:2182"
ZK_TIMEOUT = 15
ZK_ROOT = "/xmss/filecache_test/cluster_test"
REDIS_ADDRESS = "10.235.117.5:6999"

LOGGER = logging.getLogger(get_logger_name(__name__))
KAZOO_LOGGER = logging.getLogger(get_lib_logger_name(__name__))

class ZkHelperTest(unittest.TestCase):
    def test_concurrent_startup(self):
        class ZkHelperThread(threading.Thread):
            def __init__(self):
                super(ZkHelperThread, self).__init__()
                self.zk_helper = ZkHelper(ZK_ADDRESSES, ZK_TIMEOUT, ZK_ROOT, REDIS_ADDRESS)

            def run(self):
                self.zk_helper.start()
                self.zk_helper.register_redis()

            def close(self):
                self.zk_helper.close()

        zk_clients = []
        expect = []
        for i in xrange(0, 10):
            zk_clients.append(ZkHelperThread())
            expect.append(i)

        for i in xrange(0, 10):
            zk_clients[i].start()

        for i in xrange(0, 10):
            zk_clients[i].join()

        zk_children = zk_clients[0].zk_helper.get_zk_client().get_children(path="%s/pool" % ZK_ROOT)
        zk_children = [int(i) for i in zk_children]
        zk_children.sort()
        self.assertListEqual(expect, zk_children)

        for i in xrange(0, 10):
            zk_clients[i].close()

    def test_reconnect_when_zk_session_expire(self):
        self.reconnect_when_zk_session_expire(True)
        self.reconnect_when_zk_session_expire(False)

    def reconnect_when_zk_session_expire(self, is_redis_running):
        zk_helper = ZkHelper(ZK_ADDRESSES, ZK_TIMEOUT, ZK_ROOT, REDIS_ADDRESS)

        zk_helper.start()
        zk_helper.register_redis()

        # suppose redis already started
        zk_helper.set_redis_running(is_redis_running)

        # make zk session expired
        zk_client = zk_helper.get_zk_client()
        client_id = zk_client.client_id

        zk_retry = KazooRetry(max_tries=1, delay=1.0, ignore_expire=False)
        zk_client_new = KazooClient(hosts=ZK_ADDRESSES, timeout=ZK_TIMEOUT, connection_retry=zk_retry,
                                    client_id=client_id, logger=KAZOO_LOGGER)
        zk_client_new.start(ZK_TIMEOUT)
        zk_client_new.stop()
        # zk session expired done

        time.sleep(30)

        zk_children = zk_helper.get_zk_client().get_children(path="%s/pool" % ZK_ROOT)
        zk_children = [int(i) for i in zk_children]
        if is_redis_running:
            self.assertTrue(zk_helper.get_registered_redis_id() in zk_children)
        else:
            self.assertFalse(zk_helper.get_registered_redis_id() in zk_children)

        zk_helper.close()

if __name__ == "__main__":
    unittest.main()
