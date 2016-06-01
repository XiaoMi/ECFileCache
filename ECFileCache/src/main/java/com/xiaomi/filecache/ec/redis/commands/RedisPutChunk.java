package com.xiaomi.filecache.ec.redis.commands;

import com.xiaomi.filecache.ec.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisDataException;

public class RedisPutChunk extends RedisBase {
    private final String field;
    private final byte[] data;
    private final int expireTimeSec = Config.getInstance().getRedisKeyExpireSec();

    public RedisPutChunk(JedisPool jedisPool, String key, String field, byte[] data) {
        super(jedisPool, key);
        this.field = field;
        this.data = data;
        command = Command.PUT_CHUNK;
    }

    @Override
    protected int doRequest(Jedis jedis, String redisAddress) {

        Pipeline pipeline = jedis.pipelined();
        Response<Long> response = pipeline.hset(key.getBytes(), field.getBytes(), data);
        pipeline.expire(key.getBytes(), expireTimeSec);
        pipeline.sync();

        try {
            Long ret = response.get();
            if (ret != 0 && ret != 1) {
                if (LOGGER.isDebugEnabled()) {
                    String verbose = String.format("store data to redis[%s] failed. key[%s], field[%s]",
                            redisAddress, key, field);
                    LOGGER.debug(verbose);
                }
                return 1;
            }
            return 0;
        } catch (JedisDataException e) {
            String verbose = String.format("store data to redis[%s] error. key[%s], field[%s]",
                    redisAddress, key, field);
            LOGGER.warn(verbose, e);
            return 1;
        }
    }
}
