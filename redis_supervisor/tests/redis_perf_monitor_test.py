#!/usr/bin/env python
# -*- coding:utf-8 -*-
from redis_supervisor.redis_perf_monitor import RedisPerfMonitor

if __name__ == "__main__":
    monitor = RedisPerfMonitor('localhost', 6380, "ss=aws,loc=bj")
    monitor.start()
    print "started"

    monitor.join()
    print "end"
