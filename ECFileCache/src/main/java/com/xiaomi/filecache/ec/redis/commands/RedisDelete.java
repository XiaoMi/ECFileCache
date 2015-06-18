package com.xiaomi.filecache.ec.redis.commands;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisDelete extends RedisBase {

    public RedisDelete(JedisPool jedisPool, String key) {
        super(jedisPool, key);
        command = Command.DELETE;
    }

    @Override
    protected int doRequest(Jedis jedis, String redisAddress) {
        jedis.del(key.getBytes());
        return 0;
    }
}
