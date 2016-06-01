package com.xiaomi.filecache.ec.redis;

import com.xiaomi.filecache.ec.Config;
import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class DecoratedJedisPool extends JedisPool {

    private static final String ADDRESS_SEP = ":";

    static final Logger LOGGER = LoggerFactory.getLogger(DecoratedJedisPool.class.getName());

    private String host;
    private int port;

    public static DecoratedJedisPool create(String redisAddress)
            throws ECFileCacheException {
        String[] address = redisAddress.split(ADDRESS_SEP);
        if (address.length < 2) {
            String verbose = String.format("invalid redis address[%s]", redisAddress);
            LOGGER.error(verbose);
            throw new ECFileCacheException(verbose);
        }
        String host = address[0];
        int port = Integer.parseInt(address[1]);

        Validate.isTrue(StringUtils.isNotEmpty(host));
        Validate.isTrue(port > 0);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(Config.getInstance().getJedisPoolMax());

        return new DecoratedJedisPool(jedisPoolConfig, host, port);
    }

    private DecoratedJedisPool(JedisPoolConfig jedisPoolConfig, String host, int port) {
        super(jedisPoolConfig, host, port,
                Config.getInstance().getJedisConnectTimeoutMs(),
                Config.getInstance().getJedisSocketTimeoutMs(),
                Config.getInstance().getRedisPassword(),
                Protocol.DEFAULT_DATABASE, null);

        this.host = host;
        this.port = port;
    }

    public String getRedisAddress() {
        return host + ":" + port;
    }
}
