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

import org.apache.commons.collections.MapUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;

public class RedisGetAll extends RedisBase {

  private final int index;
  Map<byte[], byte[]>[] redisDataList;

  public RedisGetAll(JedisPool jedisPool, String key,
             Map<byte[], byte[]>[] redisDataList, int index) {
    super(jedisPool, key);
    this.redisDataList = redisDataList;
    this.index = index;
    command = Command.GET_ALL;
  }

  @Override
  protected int doRequest(Jedis jedis, String redisAddress) {
    Map<byte[], byte[]> redisData = jedis.hgetAll(key.getBytes());
    redisDataList[index] = redisData;
    if (MapUtils.isEmpty(redisData)) {
      String verbose = String.format("get all for key [%s] from [%s] is empty", key, redisAddress);
      LOGGER.debug(verbose);
      return 1;
    }
    return 0;
  }
}
