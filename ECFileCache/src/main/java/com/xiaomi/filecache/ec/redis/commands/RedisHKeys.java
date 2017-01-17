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

import org.apache.commons.collections.CollectionUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Set;

public class RedisHKeys extends RedisBase {
  private final int index;
  private final Set<byte[]>[] redisFields;

  public RedisHKeys(JedisPool jedisPool, String key,
            Set<byte[]>[] redisFields, int index) {
    super(jedisPool, key);
    this.redisFields = redisFields;
    this.index = index;
    command = Command.HKEYS;
  }

  @Override
  protected int doRequest(Jedis jedis, String redisAddress) {
    Set<byte[]> fields = jedis.hkeys(key.getBytes());
    redisFields[index] = fields;

    if (CollectionUtils.isEmpty(fields)) {
      String verbose = String.format("get fields [%s] from [%s] is empty", key, redisAddress);
      LOGGER.debug(verbose);
      return 1;
    }

    return 0;
  }
}
