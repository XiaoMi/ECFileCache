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
import signal
from threading import Thread
from time import sleep

__author__ = 'guoxuedong'

is_exit = False

def register_signal_handlers():
  """
  define signal processor
  """
  print "register sigterm handler"
  signal.signal(signal.SIGTERM, terminate_handler)
  signal.signal(signal.SIGINT, terminate_handler)
  print "register sigterm handler done"

def terminate_handler(signum, frame):
  global is_exit
  is_exit = True
  print "terminate event is triggered"
  register_signal_handlers()

class Foo(Thread):
  global is_exit

  def __init__(self, arg):
    super(Foo, self).__init__()
    self.id = arg

  def run(self):
    while not is_exit:
      print "running thread [%d]" % self.id
      sleep(1)

    print "terminated thread [%d]" % self.id


if __name__ == "__main__":
  register_signal_handlers()

  threads = []
  for i in xrange(0, 1):
    foo = Foo(i)
    threads.append(foo)

  for t in threads:
    t.start()

  while True:
    alive = False
    for t in threads:
      alive = alive or t.is_alive()

    if not alive:
      break

    sleep(1)

