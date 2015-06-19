package com.xiaomi.filecache.ec.redis;

import com.xiaomi.filecache.ec.ECodec;
import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import com.xiaomi.filecache.ec.utils.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public abstract class RedisAccessBase {
    public static final String SEP = "_";

    private Map<Integer, DecoratedJedisPool> keyedPool = new HashMap<Integer, DecoratedJedisPool>();

    static final Logger LOGGER = LoggerFactory.getLogger(RedisAccessBase.class.getName());

    public RedisAccessBase(Map<Integer, String> redisMap) {
        try {
            init(redisMap);
        } catch (ECFileCacheException e) {
            keyedPool.clear();
        }
    }

    public void init(Map<Integer, String> redisMap) throws ECFileCacheException {
        keyedPool.clear();
        for (Map.Entry<Integer, String> entry : redisMap.entrySet()) {
            int redisId = entry.getKey();

            String redisAddress = entry.getValue();
            DecoratedJedisPool decoratedJedisPool = DecoratedJedisPool.create(redisAddress);

            keyedPool.put(redisId, decoratedJedisPool);

            LOGGER.info("add redis[{}] to keyedPool, key[{}]", redisAddress, redisId);
        }
    }

    abstract public void put(List<Integer> redisIds, String cacheKey, long fieldKey, final byte[][] chunks) throws ECFileCacheException;

    abstract public List<Pair<byte[][], int[]>> get(List<Integer> redisIds, String cacheKey) throws ECFileCacheException;

    abstract public Pair<byte[][], int[]> getChunk(String cacheKey, long chunkPos, int chunkSize, List<Integer> redisIds)
            throws ECFileCacheException;

    abstract public Map<Long, Integer> getChunkPosAndSize(List<Integer> redisIds, String cacheKey) throws ECFileCacheException;

    abstract public void delete(String cacheKey, List<Integer> redisIds) throws ECFileCacheException;



    List<DecoratedJedisPool> getJedisPools(List<Integer> redisIds) throws ECFileCacheException {
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
            checkFail(failCount);
        }
        return jedisPools;
    }

    void checkFail(int failCount) throws ECFileCacheException {
        if (failCount > ECodec.CODING_BLOCK_NUM) {
            String verbose = String.format("get cached data fail count > CODING_BLOCK_NUM. [%d] > [%d]",
                    failCount, ECodec.CODING_BLOCK_NUM);
            LOGGER.error(verbose);
            throw new ECFileCacheException(verbose);
        }
    }

    int checkDataAndGetLength(byte[][] data) throws ECFileCacheException {
        int length = data[0].length;
        for (int i = 1, n = data.length; i < n; ++i) {
            if (data[i].length != length) {
                String verbose = "invalid put data. data length is not equal";
                LOGGER.error(verbose);
                throw new ECFileCacheException(verbose);
            }
        }
        return length;
    }

    List<Pair<byte[][], int[]>> convert(Map<byte[], byte[]>[] redisDataList) throws ECFileCacheException {
        int redisDataNum = redisDataList.length;

        Map<Long, Integer> chunkIdAndSizeMap = new TreeMap<Long, Integer>();
        List<Map<Long, byte[]>> redisDataListTemp = new ArrayList<Map<Long, byte[]>>();

        // 对从redis中读出的数据进行预处理
        // 从redis读出的数据是按照redis分组的，每组数据是一个hashmap
        // 遍历所有分组数据，得到所有的chunkId，即所有hashmap的key
        // 转换为Long并排序，为后边的拼数据做准备
        // TODO too complex
        for (Map<byte[], byte[]> map : redisDataList) {
            Map<Long, byte[]> redisDataMap = new HashMap<Long, byte[]>();
            if (MapUtils.isNotEmpty(map)) {
                for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {

                    String toks[] = new String(entry.getKey()).split(SEP);
                    Validate.isTrue(toks.length == 2);

                    long chunkId = Long.parseLong(toks[0]);
                    int chunkSize = Integer.parseInt(toks[1]);
                    if (chunkIdAndSizeMap.containsKey(chunkId) && chunkIdAndSizeMap.get(chunkId) != chunkSize) {
                        String verbose = String.format("conflict cached data of chunkId[%d]", chunkId);
                        LOGGER.error(verbose);
                        throw new ECFileCacheException(verbose);
                    }

                    chunkIdAndSizeMap.put(chunkId, chunkSize);

                    redisDataMap.put(chunkId, entry.getValue());
                }
            }
            redisDataListTemp.add(redisDataMap);
        }

        // 将以redis分组的数据转换为以chunkId为分组的数据
        // 遍历所有chunkId，从各个redis分组数据中取出对应的数据
        // 如果读出的数据为空或大小错误，则生成一个0填充的数组
        List<Pair<byte[][], int[]>> chunkAndErasuresList = new ArrayList<Pair<byte[][], int[]>>();
        long fileSize = 0;
        for (Map.Entry<Long, Integer> entry : chunkIdAndSizeMap.entrySet()) {
            Long chunkId = entry.getKey();
            int chunkSize = entry.getValue();

            // 检查数据是否完整
            if (fileSize != chunkId) {
                String verbose = String.format("lost chunk[%d], actual is [%d]", fileSize, chunkId);
                LOGGER.error(verbose);
                throw new ECFileCacheException(verbose);
            }

            fileSize += chunkSize * ECodec.DATA_BLOCK_NUM;

            byte[][] chunks = new byte[redisDataNum][];
            List<Integer> erasures = new ArrayList<Integer>();

            int i = 0;
            for (Map<Long, byte[]> map : redisDataListTemp) {
                byte[] chunk = map.get(chunkId);
                // can not read chunk data from redis
                // make a zero-filled array for ec decode
                if (ArrayUtils.isEmpty(chunk) || chunk.length != chunkSize) {
                    chunk = new byte[chunkSize];
                    erasures.add(i);
                }
                chunks[i++] = chunk;
            }
            Pair<byte[][], int[]> pair = Pair.create(chunks, adjustErasures(erasures, redisDataNum));

            chunkAndErasuresList.add(pair);
        }

        return chunkAndErasuresList;
    }


    Pair<byte[][], int[]> convertChunk(byte[][] redisDataArray, int chunkSize) {
        Validate.isTrue(!ArrayUtils.isEmpty(redisDataArray));

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

    Map<Long, Integer> convertChunkPosAndSize(Set<byte[]>[] redisFields) throws ECFileCacheException {
        Map<Long, Integer> chunkPosAndSize = new TreeMap<Long, Integer>();
        for (Set<byte[]> fields : redisFields) {
            if (CollectionUtils.isNotEmpty(fields)) {
                for (byte[] field : fields) {

                    String toks[] = new String(field).split(SEP);
                    Validate.isTrue(toks.length == 2);

                    long chunkPos = Long.parseLong(toks[0]);
                    int chunkSize = Integer.parseInt(toks[1]);

                    if (chunkPosAndSize.containsKey(chunkPos) && chunkPosAndSize.get(chunkPos) != chunkSize) {
                        String verbose = String.format("conflict cached data of chunkPos[%d]", chunkPos);
                        LOGGER.error(verbose);

                        throw new ECFileCacheException(verbose);
                    }

                    chunkPosAndSize.put(chunkPos, chunkSize);
                }
            }
        }

        long fileSize = 0;
        for (Map.Entry<Long, Integer> entry : chunkPosAndSize.entrySet()) {
            long chunkPos = entry.getKey();
            int chunkSize = entry.getValue();

            // 检查数据是否完整
            if (fileSize != chunkPos) {
                String verbose = String.format("lost chunk[%d], actual is [%d]", fileSize, chunkPos);
                LOGGER.error(verbose);
                throw new ECFileCacheException(verbose);
            }

            fileSize += chunkSize * ECodec.DATA_BLOCK_NUM;
        }

        return chunkPosAndSize;
    }

    private int[] adjustErasures(List<Integer> erasures, int ecBlockNum) {
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


    public int size() {
        return keyedPool.size();
    }

    public Map<Integer, DecoratedJedisPool> getKeyedPool() {
        return keyedPool;
    }
}
