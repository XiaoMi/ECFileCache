// Copyright 2016 Xiaomi, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.xiaomi.filecache.ec.redis.commands;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

public class RedisGetInfo extends RedisBase {
  private List<byte[]> result;

  public RedisGetInfo(JedisPool jedisPool, String key, List<byte[]> result) {
    super(jedisPool, key);
    this.result = result;
    command = Command.GET_INFO;
  }

  @Override
  protected int doRequest(Jedis jedis, String redisAddress) {
    byte[] redisData = jedis.get(key.getBytes());
    result.add(redisData);
    if (result.isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        String verbose = String.format("get info for key [%s] from [%s] is empty", key, redisAddress);
        LOGGER.debug(verbose);
      }
      return 1;
    }
    return 0;
  }
}
