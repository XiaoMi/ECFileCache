package com.xiaomi.filecache.ec.redis.commands;

import com.xiaomi.filecache.ec.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisPutChunk extends RedisBase {
    private final String field;
    private final byte[] data;
    private final boolean needSetExpire;

    public RedisPutChunk(JedisPool jedisPool, String key, String field, byte[] data, boolean needSetExpire) {
        super(jedisPool, key);
        this.field = field;
        this.data = data;
        this.needSetExpire = needSetExpire;
        command = Command.PUT_CHUNK;
    }

    @Override
    protected int doRequest(Jedis jedis, String redisAddress) {

        Long ret = jedis.hset(key.getBytes(), field.getBytes(), data);
        if (ret != 0 && ret != 1) {
            String verbose = String.format("store data to redis[%s] error. key[%s], field[%s]",
                    redisAddress, key, field);
            LOGGER.debug(verbose);
            return 1;
        }

        if (needSetExpire) {
            int expireTimeSec = Config.getInstance().getRedisKeyExpireSec();
            jedis.expire(key.getBytes(), expireTimeSec);
            String verbose = String.format("set expire for key [%s] to [%d] second, in redis[%s]", key, expireTimeSec, redisAddress);
            LOGGER.debug(verbose);
        }
        return 0;
    }
}
