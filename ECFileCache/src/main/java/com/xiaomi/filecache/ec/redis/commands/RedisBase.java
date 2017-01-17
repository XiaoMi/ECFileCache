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

import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import com.xiaomi.filecache.ec.redis.DecoratedJedisPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.Callable;

public abstract class RedisBase implements Callable<Integer> {
  final DecoratedJedisPool jedisPool;
  final String key;

  enum Command {
    DELETE,
    GET_ALL,
    GET_CHUNK,
    HKEYS,
    PUT_CHUNK,
    PUT_INFO,
    GET_INFO
  }

  Command command = null;

  static final Logger LOGGER = LoggerFactory.getLogger(RedisBase.class.getName());

  public RedisBase(JedisPool jedisPool, String key) {
    this.jedisPool = (DecoratedJedisPool) jedisPool;
    this.key = key;
  }

  @Override
  public Integer call() throws Exception {
    if (jedisPool == null) {
      String verbose = String.format("have no jedis pool for key[%s], command[%s]", key, command.toString());
      LOGGER.warn(verbose);
      throw new ECFileCacheException(verbose);
    }

    Jedis jedis = null;
    long start = System.currentTimeMillis();
    try {
      jedis = jedisPool.getResource();
      start = System.currentTimeMillis();
      return doRequest(jedis, jedisPool.getRedisAddress());
    } catch (Exception e) {
      long cost = System.currentTimeMillis() - start;
      if (jedis != null) {
        jedis.close();
        jedis = null;
      }
      String verbose = String.format("Access redis [%s] for key [%s] and command [%s] exception: [%s], cost [%d]",
          jedisPool.getRedisAddress(), key, command.toString(), e.getMessage(), cost);
      LOGGER.warn(verbose);
      throw new ECFileCacheException(verbose, e);
    } finally {
      if (jedis != null) {
        jedisPool.returnResourceObject(jedis);
      }
    }
  }

  protected abstract int doRequest(Jedis jedis, String redisAddress);
}
