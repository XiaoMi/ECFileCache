// Copyright 2016 Xiaomi, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

  /**
   * Constructs Redis access. Create Redis pool for all Redis
   *
   * @param redisMap Redis index and address
   */
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

  /**
   * Put chunk data of file to cache
   *
   * @param redisIds Redis indexes indicate which Redis to use
   * @param cacheKey key of cached file
   * @param chunkPos chunk offset in file, used as chunk key
   * @param chunks chunk of file to cache
   * @throws ECFileCacheException
   */
  public abstract void put(List<Integer> redisIds, String cacheKey, long chunkPos, final byte[][] chunks) throws ECFileCacheException;

  /**
   * Get file data from cache
   *
   * @param redisIds Redis indexes indicate which Redis to use
   * @param cacheKey key of cached file
   * @return all chunks of file. need to reorder and decode to get the origin file
   * @throws ECFileCacheException
   */
  public abstract List<Pair<byte[][], int[]>> get(List<Integer> redisIds, String cacheKey) throws ECFileCacheException;

  /**
   * Get a specified chunk from cache
   *
   * @param redisIds Redis indexes indicate which Redis to use
   * @param cacheKey key of cached file
   * @param chunkPos chunk offset in file, used as chunk key
   * @param chunkSize length of chunk data
   * @return
   * @throws ECFileCacheException
   */
  public abstract Pair<byte[][], int[]> getChunk(List<Integer> redisIds, String cacheKey, long chunkPos, int chunkSize)
      throws ECFileCacheException;

  /**
   * Get all chunks' info, named offset and size of chunk
   *
   * @param redisIds Redis indexes indicate which Redis to use
   * @param cacheKey key of cached file
   * @return all chunks' offset and size. The key of returned map is chunk offset, value is chunk size
   * @throws ECFileCacheException
   */
  public abstract Map<Long, Integer> getChunkPosAndSize(List<Integer> redisIds, String cacheKey) throws ECFileCacheException;

  /**
   * Delete cached data of key
   *
   * @param redisIds Redis indexes indicate which Redis to use
   * @param cacheKey key of cached file
   * @throws ECFileCacheException
   */
  public abstract void delete(List<Integer> redisIds, String cacheKey) throws ECFileCacheException;

  /**
   * Put info data to a cache node
   * Use only one Redis, info may be lost when redis restart
   * So do not save important data by this method
   *
   * @param redisId Redis index indicate which Redis to use
   * @param cacheKey key of cache info
   * @param data cache info
   */
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

  /**
   * Get info from cache node
   *
   * @param redisId Redis index indicate which Redis to use
   * @param cacheKey key of cache info
   * @return cached info data, maybe null
   */
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

  /**
   * Check whether times of access Redis failed is bigger than ECodec.CODING_BLOCK_NUM
   *
   * @param failCount times of access Redis failed
   * @param method method name for log
   * @param key the key of cached data
   * @throws ECFileCacheException
   */
  static void checkFail(int failCount, String method, String key) throws ECFileCacheException {
    if (failCount > ECodec.CODING_BLOCK_NUM) {
      String verbose = String.format("[%s]:get cached data key[%s] fail count > CODING_BLOCK_NUM. [%d] > [%d]",
          method, key, failCount, ECodec.CODING_BLOCK_NUM);
      LOGGER.error(verbose);
      throw new ECFileCacheException(verbose);
    }
  }

  /**
   * Check every row's length of 2D array is same
   *
   * @param data 2D array
   * @return row's length of 2D array
   * @throws ECFileCacheException
   */
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

  /**
   * Convert data read from redis to list
   *
   * @param redisDataList  array of Redis hash map that read from Redis nodes
   * @return list with pair element of chunk data and erasures info
   * @throws ECFileCacheException
   */
  static List<Pair<byte[][], int[]>> convert(Map<byte[], byte[]>[] redisDataList) throws ECFileCacheException {
    int redisDataNum = redisDataList.length;

    @SuppressWarnings("unchecked")
    Set<byte[]>[] redisFields = new Set[redisDataNum];
    List<Map<String, byte[]>> redisDataListTemp = new ArrayList<Map<String, byte[]>>();

    // pre-process redis data
    // get hashmap from every redis
    // get all hashmap key, named all chunkId and chunkSize
    // convert to Long and sort
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

    // process redis data by chunkId
    // generate a zero-filled array if chunk data is empty or chunk size is not match
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

  /**
   * Convert chunks get from Redis.
   * Make a zero-filled array if failed to read chunk from Redis.
   *
   * @param redisDataArray chunks get from all Redis nodes
   * @param chunkSize length of chunk data
   * @return pair of chunks and erasures array
   */
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

  /**
   * Convert chunks info read from Redis to sorted map.
   *
   * @param redisFields chunks info get from Redis
   * @return map, with chunk offset as key and chunk size of value
   * @throws ECFileCacheException
   */
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

  /**
   * Convert chunk info from Redis nodes to map.
   *
   * @param redisFields chunk offset and size info get from Redis. In format chunkPos_chunkSize.
   * @return map, with chunk info as map key and chunk offset as value
   */
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

    Map<String, Long> chunkInfoAndPos = new HashMap<String, Long>();
    for (Map.Entry<String, Integer> entry : chunkPosAndSizeCount.entrySet()) {
      if (entry.getValue() >= ECodec.DATA_BLOCK_NUM) {

        String[] toks = entry.getKey().split(SEP);
        Validate.isTrue(toks.length == 2);

        long chunkPos = Long.parseLong(toks[0]);
        chunkInfoAndPos.put(entry.getKey(), chunkPos);
      }
    }

    return chunkInfoAndPos;
  }

  /**
   * Check chunks is continuous and have no overlap
   *
   * @param chunkPosAndSize chunks data
   * @throws ECFileCacheException
   */
  static void checkChunks(Map<Long, Integer> chunkPosAndSize) throws ECFileCacheException {
    long nextChunkPos = 0;
    for (Map.Entry<Long, Integer> entry : chunkPosAndSize.entrySet()) {
      long chunkPos = entry.getKey();
      int chunkSize = entry.getValue();

      if (nextChunkPos != chunkPos) {
        String verbose = String.format("lost chunk[%d], actual is [%d]", nextChunkPos, chunkPos);
        LOGGER.error(verbose);
        throw new ECFileCacheException(verbose);
      }

      nextChunkPos += chunkSize * ECodec.DATA_BLOCK_NUM;
    }
  }

  /**
   * Convert erasures info list to array
   *
   * @param erasures list with int element, save erased chunk index
   * @param ecBlockNum chunks' number
   * @return erasures array indicated which chunk was erased
   */
  static int[] adjustErasures(List<Integer> erasures, int ecBlockNum) {
    // erasures array should contain at least one element, required by libjerasure
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
