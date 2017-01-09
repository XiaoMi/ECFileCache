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
