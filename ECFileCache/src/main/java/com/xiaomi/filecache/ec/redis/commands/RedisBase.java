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
        PUT_CHUNK
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
            LOGGER.error(verbose);
            throw new ECFileCacheException(verbose);
        }

        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return doRequest(jedis, jedisPool.getRedisAddress());
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            String verbose = String.format("Access redis [%s] for key [%s] and command [%s] exception: [%s]",
                    jedisPool.getRedisAddress(), key, command.toString(), e.getMessage());
            LOGGER.error(verbose);
            throw new ECFileCacheException(verbose, e);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }
        }
    }

    protected abstract int doRequest(Jedis jedis, String redisAddress);
}
