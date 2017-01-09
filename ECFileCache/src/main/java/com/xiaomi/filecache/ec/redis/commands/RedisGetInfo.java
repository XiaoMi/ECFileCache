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
