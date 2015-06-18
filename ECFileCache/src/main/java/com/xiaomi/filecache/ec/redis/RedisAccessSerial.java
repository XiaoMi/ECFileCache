package com.xiaomi.filecache.ec.redis;

import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import com.xiaomi.filecache.ec.redis.commands.RedisBase;
import com.xiaomi.filecache.ec.redis.commands.RedisDelete;
import com.xiaomi.filecache.ec.redis.commands.RedisGetAll;
import com.xiaomi.filecache.ec.redis.commands.RedisGetChunk;
import com.xiaomi.filecache.ec.redis.commands.RedisHKeys;
import com.xiaomi.filecache.ec.redis.commands.RedisPutChunk;
import com.xiaomi.filecache.ec.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisAccessSerial extends RedisAccessBase {

    public RedisAccessSerial(Map<Integer, String> redisMap) {
        super(redisMap);
    }

    @Override
    public void put(List<Integer> redisIds, String cacheKey, long fieldKey, final byte[][] chunks) throws ECFileCacheException {

        long dataLength = checkDataAndGetLength(chunks);
        List<DecoratedJedisPool> jedisPools = getJedisPools(redisIds);

        int failCount = 0;
        for (int i = 0; i < jedisPools.size(); ++i) {
            DecoratedJedisPool jedis = jedisPools.get(i);
            if (jedis != null) {
                final String key = cacheKey + SEP + Integer.toString(i);
                final String field = fieldKey + SEP + dataLength;
                final byte[] data = chunks[i];
                RedisPutChunk redisPutChunk = new RedisPutChunk(jedis, key, field, data, fieldKey == 0);

                failCount += accessRedisAndCheckResult(redisPutChunk, key);
            } else {
                failCount++;
            }
            checkFail(failCount);
        }
    }

    @Override
    public List<Pair<byte[][], int[]>> get(List<Integer> redisIds, String cacheKey) throws ECFileCacheException {

        List<DecoratedJedisPool> jedisPools = getJedisPools(redisIds);

        Map<byte[], byte[]>[] redisDataList = new Map[redisIds.size()];

        int failCount = 0;
        for (int i = 0; i < jedisPools.size(); ++i) {
            DecoratedJedisPool jedis = jedisPools.get(i);
            if (jedis != null) {
                final String key = cacheKey + SEP + i;
                RedisGetAll redisGetAll = new RedisGetAll(jedis, key, redisDataList, i);

                failCount += accessRedisAndCheckResult(redisGetAll, key);
            } else {
                redisDataList[i] = null;
                failCount++;
            }
            checkFail(failCount);
        }

        return convert(redisDataList);
    }

    @Override
    public Pair<byte[][], int[]> getChunk(String cacheKey, long chunkPos, int chunkSize, List<Integer> redisIds)
            throws ECFileCacheException {

        List<DecoratedJedisPool> jedisPools = getJedisPools(redisIds);

        byte[][] redisDataArray = new byte[jedisPools.size()][];

        String field = chunkPos + SEP + chunkSize;
        int failCount = 0;
        for (int i = 0; i < jedisPools.size(); ++i) {
            DecoratedJedisPool jedis = jedisPools.get(i);
            if (jedis != null) {
                String key = cacheKey + SEP + i;
                RedisGetChunk redisGetChunk = new RedisGetChunk(jedis, key, field, redisDataArray, i);

                failCount += accessRedisAndCheckResult(redisGetChunk, key);
            } else {
                redisDataArray[i] = null;
                failCount++;
            }
            checkFail(failCount);
        }
        return convertChunk(redisDataArray, chunkSize);
    }

    @Override
    public Map<Long, Integer> getChunkPosAndSize(List<Integer> redisIds, String cacheKey) throws ECFileCacheException {

        List<DecoratedJedisPool> jedisPools = getJedisPools(redisIds);
        Set<byte[]>[] redisFields = new Set[jedisPools.size()];

        int failCount = 0;
        for (int i = 0; i < jedisPools.size(); ++i) {

            DecoratedJedisPool jedis = jedisPools.get(i);
            if (jedis != null) {
                final String key = cacheKey + SEP + Integer.toString(i);
                RedisHKeys redisHKeys = new RedisHKeys(jedis, key, redisFields, i);

                failCount += accessRedisAndCheckResult(redisHKeys, key);
            } else {
                redisFields[i] = null;
                failCount++;
            }
            checkFail(failCount);
        }
        return convertChunkPosAndSize(redisFields);
    }

    @Override
    public void delete(String cacheKey, List<Integer> redisIds) throws ECFileCacheException {

        List<DecoratedJedisPool> jedisPools = getJedisPools(redisIds);
        for (int i = 0; i < jedisPools.size(); ++i) {

            String key = cacheKey + SEP + Integer.toString(i);
            RedisDelete redisDelete = new RedisDelete(jedisPools.get(i), key);
            try {
                redisDelete.call();
            } catch (Exception e) {
                String verbose = "delete data fail for key:" + key;
                LOGGER.warn(verbose);
            }
        }
    }

    private int accessRedisAndCheckResult(RedisBase redisCommand, String key) throws ECFileCacheException {
        try {
            if (0 == redisCommand.call()) {
                return 0;
            }
        } catch (Exception e) {
            String verbose = "access redis fail for key:" + key;
            LOGGER.error(verbose, e.getMessage());
        }

        return 1;
    }
}
