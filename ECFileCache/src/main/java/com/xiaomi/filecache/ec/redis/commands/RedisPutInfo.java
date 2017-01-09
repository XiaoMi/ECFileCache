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
