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

import org.apache.commons.lang3.ArrayUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisGetChunk extends RedisBase {
  private final String field;
  private final byte[][] redisDataList;
  private final int index;

  public RedisGetChunk(JedisPool jedisPool, String key, String field, byte[][] redisDataList, int index) {
    super(jedisPool, key);
    this.field = field;
    this.redisDataList = redisDataList;
    this.index = index;
    command = Command.GET_CHUNK;
  }

  @Override
  protected int doRequest(Jedis jedis, String redisAddress) {
    byte[] redisData = jedis.hget(key.getBytes(), field.getBytes());
    redisDataList[index] = redisData;
    if (ArrayUtils.isEmpty(redisData)) {
      String verbose = String.format("get chunk for key [%s] from [%s] is empty", key, redisAddress);
      LOGGER.debug(verbose);
      return 1;
    }
    return 0;
  }
}
