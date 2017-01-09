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
