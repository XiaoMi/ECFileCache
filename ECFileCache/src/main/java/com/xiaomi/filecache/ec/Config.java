package com.xiaomi.filecache.ec;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Config {

    static final Logger LOGGER = LoggerFactory.getLogger(Config.class.getName());

    private static final String SELECT_OFFSET_MAX_RETRY = "select_offset_max_retry";
    private static final String TOLERATE_ONE_ERASED_DEVICE_AFTER_RETRY = "tolerate_one_erased_device_after_retry";
    private static final String JEDIS_POOL_MAX = "jedis_pool_max";
    private static final String JEDIS_SOCKET_TIMEOUT_MS = "jedis_socket_timeout_ms";
    private static final String CHECK_JEDIS_RESULT_TIMEOUT_MS = "check_jedis_result_timeout_ms";
    private static final String REDIS_PASSWORD = "redis_password";
    private static final String REDIS_KEY_EXPIRE_SEC= "redis_key_expire_sec";
    private static final String REDIS_ACCESS_PARALLEL= "redis_access_parallel";
    private static final String REDIS_ACCESS_THREAD_NUM= "redis_access_thread_num";

    private int selectOffsetMaxRetry;
    private int tolerateOneErasedDeviceAfterRetry;
    private int jedisPoolMax;
    private int jedisSocketTimeoutMs;
    private int checkJedisResultTimeoutMs;
    private int redisKeyExpireSec;
    private boolean redisAccessParallel;
    private int redisAccessThreadNum;
    private String redisPassword;

    private volatile static Config config = null;

    public static void init(Properties props) {
        if (config == null) {
            synchronized (Config.class) {
                if (config == null) {
                    config = new Config(props);
                    LOGGER.info("init config done:" + config.toString());
                }
            }
        }
    }

    public static Config getInstance() {
        Validate.isTrue(config != null);
        return config;
    }

    private Config(Properties props) {
        selectOffsetMaxRetry = Integer.parseInt(props.getProperty(SELECT_OFFSET_MAX_RETRY, "5"));
        tolerateOneErasedDeviceAfterRetry = Integer.parseInt(props.getProperty(TOLERATE_ONE_ERASED_DEVICE_AFTER_RETRY, "3"));
        Validate.isTrue(tolerateOneErasedDeviceAfterRetry > 0 && selectOffsetMaxRetry >= tolerateOneErasedDeviceAfterRetry);

        jedisPoolMax = Integer.parseInt(props.getProperty(JEDIS_POOL_MAX, "20"));
        jedisSocketTimeoutMs = Integer.parseInt(props.getProperty(JEDIS_SOCKET_TIMEOUT_MS, "200"));
        checkJedisResultTimeoutMs = Integer.parseInt(props.getProperty(CHECK_JEDIS_RESULT_TIMEOUT_MS, "30"));
        Validate.isTrue((jedisPoolMax | jedisSocketTimeoutMs | checkJedisResultTimeoutMs) > 0);

        redisKeyExpireSec = Integer.parseInt(props.getProperty(REDIS_KEY_EXPIRE_SEC, "3600"));

        redisAccessParallel = Boolean.parseBoolean(props.getProperty(REDIS_ACCESS_PARALLEL, "false"));

        redisAccessThreadNum = Integer.parseInt(props.getProperty(REDIS_ACCESS_THREAD_NUM, "12"));
        if (redisAccessParallel) {
            Validate.isTrue(redisAccessThreadNum > 0);
        }

        redisPassword = props.getProperty(REDIS_PASSWORD, null);
    }

    public int getSelectOffsetMaxRetry() {
        return selectOffsetMaxRetry;
    }

    public int getTolerateOneErasedDeviceAfterRetry() {
        return tolerateOneErasedDeviceAfterRetry;
    }

    public int getJedisPoolMax() {
        return jedisPoolMax;
    }

    public int getJedisSocketTimeoutMs() {
        return jedisSocketTimeoutMs;
    }

    public int getCheckJedisResultTimeoutMs() {
        return checkJedisResultTimeoutMs;
    }

    public String getRedisPassword() {
        return redisPassword;
    }

    public int getRedisKeyExpireSec() {
        return redisKeyExpireSec;
    }

    public boolean isRedisAccessParallel() {
        return redisAccessParallel;
    }

    public int getRedisAccessThreadNum() {
        return redisAccessThreadNum;
    }

    @Override
    public String toString() {
        return "Config{" +
                "selectOffsetMaxRetry=" + selectOffsetMaxRetry +
                ", tolerateOneErasedDeviceAfterRetry=" + tolerateOneErasedDeviceAfterRetry +
                ", jedisPoolMax=" + jedisPoolMax +
                ", jedisSocketTimeoutMs=" + jedisSocketTimeoutMs +
                ", checkJedisResultTimeoutMs=" + checkJedisResultTimeoutMs +
                ", redisKeyExpireSec=" + redisKeyExpireSec +
                ", redisAccessParallel=" + redisAccessParallel +
                ", redisAccessThreadNum=" + redisAccessThreadNum +
                '}';
    }
}
