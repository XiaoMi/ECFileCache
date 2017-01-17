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

import com.xiaomi.filecache.ec.Config;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisDataException;

public class RedisPutInfo extends RedisBase {
  private final byte[] data;
  private int expireTimeSec;

  public RedisPutInfo(JedisPool jedisPool, String key, byte[] data) {
    super(jedisPool, key);
    this.data = data;
    command = Command.PUT_INFO;
    expireTimeSec = Config.getInstance().getRedisKeyExpireSec();
  }

  @Override
  protected int doRequest(Jedis jedis, String redisAddress) {

    Pipeline pipeline = jedis.pipelined();
    Response<String> response = pipeline.set(key.getBytes(), data);
    pipeline.expire(key.getBytes(), expireTimeSec);
    pipeline.sync();

    try {
      String ret = response.get();
      if (!StringUtils.equalsIgnoreCase(ret, "OK")) {
        if (LOGGER.isDebugEnabled()) {
          String verbose = String.format("store info to redis[%s] error. key[%s]",
              redisAddress, key);
          LOGGER.debug(verbose);
        }
        return 1;
      }
      return 0;
    } catch (JedisDataException e) {
      String verbose = String.format("store info to redis[%s] error. key[%s]",
          redisAddress, key);
      LOGGER.warn(verbose, e);
      return 1;
    }
  }
}
