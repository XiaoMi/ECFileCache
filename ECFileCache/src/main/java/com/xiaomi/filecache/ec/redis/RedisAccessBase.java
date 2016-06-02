package com.xiaomi.filecache.ec.redis;

import com.xiaomi.filecache.ec.ECodec;
import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import com.xiaomi.filecache.ec.redis.commands.RedisGetInfo;
import com.xiaomi.filecache.ec.redis.commands.RedisPutInfo;
import com.xiaomi.filecache.ec.utils.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public abstract class RedisAccessBase {
    public static final String SEP = "_";
    private static final String INFO = "info";

    public static final String GET_JEDIS_POOLS = "getJedisPools";
    public static final String GET_CHUNK_POS_AND_SIZE = " getChunkPosAndSize";
    public static final String GET_CHUNK = "getChunk";
    public static final String GET = "get";
    public static final String PUT = "put";
    public static final String CHECK_REDIS_RESULT = "checkRedisResult";

    private Map<Integer, DecoratedJedisPool> keyedPool = new HashMap<Integer, DecoratedJedisPool>();

    static final Logger LOGGER = LoggerFactory.getLogger(RedisAccessBase.class.getName());

    public RedisAccessBase(Map<Integer, String> redisMap) {
        try {
            keyedPool.clear();
            for (Map.Entry<Integer, String> entry : redisMap.entrySet()) {
                int redisId = entry.getKey();

                String redisAddress = entry.getValue();
                DecoratedJedisPool decoratedJedisPool = DecoratedJedisPool.create(redisAddress);

                keyedPool.put(redisId, decoratedJedisPool);

                LOGGER.info("add redis[{}] to keyedPool, key[{}]", redisAddress, redisId);
            }
        } catch (ECFileCacheException e) {
            keyedPool.clear();
        }
    }

    public abstract void put(List<Integer> redisIds, String cacheKey, long fieldKey, final byte[][] chunks) throws ECFileCacheException;

    public abstract List<Pair<byte[][], int[]>> get(List<Integer> redisIds, String cacheKey) throws ECFileCacheException;

    public abstract Pair<byte[][], int[]> getChunk(String cacheKey, long chunkPos, int chunkSize, List<Integer> redisIds)
            throws ECFileCacheException;

    public abstract Map<Long, Integer> getChunkPosAndSize(List<Integer> redisIds, String cacheKey) throws ECFileCacheException;

    public abstract void delete(String cacheKey, List<Integer> redisIds) throws ECFileCacheException;

    public void putInfo(int redisId, String cacheKey, final byte[] data) {
        DecoratedJedisPool jedisPool = keyedPool.get(redisId);
        if (jedisPool == null) {
            String verbose = String.format("put info: have no jedis pool of id[%d]", redisId);
            LOGGER.warn(verbose);
            return;
        }

        cacheKey = cacheKey + SEP + INFO;
        RedisPutInfo redisPutInfo = new RedisPutInfo(jedisPool, cacheKey, data);

        try {
            redisPutInfo.call();
        } catch (Exception ignore) {
            // ignore exception
        }
    }

    /*@ Nullable */
    public byte[] getInfo(int redisId, String cacheKey) {
        DecoratedJedisPool jedisPool = keyedPool.get(redisId);
        if (jedisPool == null) {
            String verbose = String.format("get info: have no jedis pool of id[%d]", redisId);
            LOGGER.warn(verbose);
            return null;
        }

        cacheKey = cacheKey + SEP + INFO;
        List<byte[]> result = new ArrayList<byte[]>(1);
        RedisGetInfo redisGetInfo = new RedisGetInfo(jedisPool, cacheKey, result);
        try {
            if (0 == redisGetInfo.call()) {
                return result.get(0);
            }
        } catch (Exception ignore) {
            // ignore exception
        }
        return null;
    }

    protected List<DecoratedJedisPool> getJedisPools(List<Integer> redisIds) throws ECFileCacheException {
        List<DecoratedJedisPool> jedisPools = new ArrayList<DecoratedJedisPool>();
        int failCount = 0;
        for (Integer redisId : redisIds) {

            DecoratedJedisPool jedisPool = keyedPool.get(redisId);
            jedisPools.add(jedisPool);

            if (jedisPool == null) {
                String verbose = String.format("have no jedis pool of id[%d]", redisId);
                LOGGER.warn(verbose);
                failCount++;
            }
            checkFail(failCount, GET_JEDIS_POOLS, "NIL");
        }
        return jedisPools;
    }

    public Map<Integer, DecoratedJedisPool> getKeyedPool() {
        return keyedPool;
    }

    static void checkFail(int failCount, String method, String key) throws ECFileCacheException {
        if (failCount > ECodec.CODING_BLOCK_NUM) {
            String verbose = String.format("[%s]:get cached data key[%s] fail count > CODING_BLOCK_NUM. [%d] > [%d]",
                    method, key, failCount, ECodec.CODING_BLOCK_NUM);
            LOGGER.error(verbose);
            throw new ECFileCacheException(verbose);
        }
    }

    static int checkDataAndGetLength(byte[][] data) throws ECFileCacheException {
        int length = -1;
        for (final byte[] aData : data) {
            if (ArrayUtils.isEmpty(aData)) {
                String verbose = "invalid put data. empty data";
                LOGGER.error(verbose);
                throw new ECFileCacheException(verbose);
            }

            if (length == -1) {
                length = aData.length;
            } else if (aData.length != length) {
                String verbose = "invalid put data. data length is not equal";
                LOGGER.error(verbose);
                throw new ECFileCacheException(verbose);
            }
        }
        return length;
    }

    static List<Pair<byte[][], int[]>> convert(Map<byte[], byte[]>[] redisDataList) throws ECFileCacheException {
        int redisDataNum = redisDataList.length;

        @SuppressWarnings("unchecked")
        Set<byte[]>[] redisFields = new Set[redisDataNum];
        List<Map<String, byte[]>> redisDataListTemp = new ArrayList<Map<String, byte[]>>();

        // 对从redis中读出的数据进行预处理
        // 从redis读出的数据是按照redis分组的，每组数据是一个hashmap
        // 遍历所有分组数据，得到所有的chunkId和chunkSize，即所有hashmap的key
        // 转换为Long并排序，为后边的拼数据做准备
        for (int i = 0; i < redisDataNum; ++i) {
            Set<byte[]> fieldSet = new HashSet<byte[]>();
            Map<String, byte[]> redisDataMap = new HashMap<String, byte[]>();

            Map<byte[], byte[]> map = redisDataList[i];
            if (MapUtils.isNotEmpty(map)) {
                for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                    fieldSet.add(entry.getKey());

                    String chunkPosAndSizeStr = new String(entry.getKey());
                    redisDataMap.put(chunkPosAndSizeStr, entry.getValue());
                }
            }
            redisFields[i] = fieldSet;
            redisDataListTemp.add(redisDataMap);
        }

        Map<Long, Integer> chunkPosAndSize = convertChunkPosAndSize(redisFields);

        // 将以redis分组的数据转换为以chunkId为分组的数据
        // 遍历所有chunkId，从各个redis分组数据中取出对应的数据
        // 如果读出的数据为空或大小错误，则生成一个0填充的数组
        List<Pair<byte[][], int[]>> chunkAndErasuresList = new ArrayList<Pair<byte[][], int[]>>();
        for (Map.Entry<Long, Integer> entry : chunkPosAndSize.entrySet()){
            Long chunkId = entry.getKey();
            int chunkSize = entry.getValue();
            String chunkPosAndSizeStr = chunkId + SEP + chunkSize;

            byte[][] chunks = new byte[redisDataNum][];

            int i = 0;
            for (Map<String, byte[]> map : redisDataListTemp) {
                chunks[i++] = map.get(chunkPosAndSizeStr);
            }
            Pair<byte[][], int[]> pair = convertChunk(chunks, chunkSize);
            chunkAndErasuresList.add(pair);
        }

        return chunkAndErasuresList;
    }

    static Pair<byte[][], int[]> convertChunk(byte[][] redisDataArray, int chunkSize) {
        Validate.isTrue(ArrayUtils.isNotEmpty(redisDataArray));

        List<Integer> erasures = new ArrayList<Integer>();

        int i = 0;
        for (byte[] chunk : redisDataArray) {
            // can not read chunk data from redis
            // make a zero-filled array for ec decode
            if (ArrayUtils.isEmpty(chunk) || chunk.length != chunkSize) {
                chunk = new byte[chunkSize];
                erasures.add(i);
            }
            redisDataArray[i++] = chunk;
        }
        return Pair.create(redisDataArray, adjustErasures(erasures, redisDataArray.length));
    }

    static Map<Long, Integer> convertChunkPosAndSize(Set<byte[]>[] redisFields) throws ECFileCacheException {

        Map<String, Long> chunkPosSizeMap = getValidateChunks(redisFields);
        Set<Long> chunkPosSet = new HashSet<Long>(chunkPosSizeMap.values());

        Map<Long, Integer> chunkPosAndSize = new TreeMap<Long, Integer>();

        for (String chunkPosAndSizeStr : chunkPosSizeMap.keySet()) {

            String[] toks = chunkPosAndSizeStr.split(SEP);
            Validate.isTrue(toks.length == 2);

            long chunkPos = Long.parseLong(toks[0]);
            int chunkSize = Integer.parseInt(toks[1]);

            if (!chunkPosAndSize.containsKey(chunkPos) ||
                    (chunkPosAndSize.get(chunkPos) > chunkSize && chunkPosSet.contains(chunkPos + chunkSize * ECodec.DATA_BLOCK_NUM))) {
                chunkPosAndSize.put(chunkPos, chunkSize);
            }
        }

        checkChunks(chunkPosAndSize);

        return chunkPosAndSize;
    }

    static Map<String, Long> getValidateChunks(Set<byte[]>[] redisFields) {
        Map<String, Integer> chunkPosAndSizeCount = new HashMap<String, Integer>();
        for (Set<byte[]> fields : redisFields) {
            if (CollectionUtils.isEmpty(fields)) {
                continue;
            }

            for (byte[] field : fields) {
                String chunkPosAndSizeStr = new String(field);
                if (chunkPosAndSizeCount.containsKey(chunkPosAndSizeStr)) {
                    chunkPosAndSizeCount.put(chunkPosAndSizeStr, chunkPosAndSizeCount.get(chunkPosAndSizeStr) + 1);
                } else {
                    chunkPosAndSizeCount.put(chunkPosAndSizeStr, 1);
                }
            }
        }

        Map<String, Long> chunkPosAndSize = new HashMap<String, Long>();
        for (Map.Entry<String, Integer> entry : chunkPosAndSizeCount.entrySet()) {
            if (entry.getValue() >= ECodec.DATA_BLOCK_NUM) {

                String[] toks = entry.getKey().split(SEP);
                Validate.isTrue(toks.length == 2);

                long chunkPos = Long.parseLong(toks[0]);
                chunkPosAndSize.put(entry.getKey(), chunkPos);
            }
        }

        return chunkPosAndSize;
    }

    static void checkChunks(Map<Long, Integer> chunkPosAndSize) throws ECFileCacheException {
        long nextChunkPos = 0;
        for (Map.Entry<Long, Integer> entry : chunkPosAndSize.entrySet()) {
            long chunkPos = entry.getKey();
            int chunkSize = entry.getValue();

            // 检查数据是否完整且不重复
            if (nextChunkPos != chunkPos) {
                String verbose = String.format("lost chunk[%d], actual is [%d]", nextChunkPos, chunkPos);
                LOGGER.error(verbose);
                throw new ECFileCacheException(verbose);
            }

            nextChunkPos += chunkSize * ECodec.DATA_BLOCK_NUM;
        }
    }

    static int[] adjustErasures(List<Integer> erasures, int ecBlockNum) {
        // jerasure库需要erasures数组中至少有一个元素
        if (erasures.isEmpty()) {
            erasures.add(ecBlockNum - 1);
        }
        int[] erasuresInt = new int[erasures.size()];
        for (int i = 0, len = erasures.size(); i < len; ++i) {
            erasuresInt[i] = erasures.get(i);
        }
        return erasuresInt;
    }
}
