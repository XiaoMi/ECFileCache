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
