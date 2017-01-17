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

import com.xiaomi.filecache.ec.Config;
import com.xiaomi.filecache.ec.ECodec;
import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import com.xiaomi.filecache.ec.redis.commands.RedisDelete;
import com.xiaomi.filecache.ec.redis.commands.RedisGetAll;
import com.xiaomi.filecache.ec.redis.commands.RedisGetChunk;
import com.xiaomi.filecache.ec.redis.commands.RedisHKeys;
import com.xiaomi.filecache.ec.redis.commands.RedisPutChunk;
import com.xiaomi.filecache.ec.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RedisAccessParallel extends RedisAccessBase {
  private final int checkResultTimeoutMs;

  private ThreadPoolExecutor pool;

  public RedisAccessParallel(Map<Integer, String> redisMap) {
    super(redisMap);
    this.checkResultTimeoutMs = Config.getInstance().getCheckJedisResultTimeoutMs();

    pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Config.getInstance().getRedisAccessThreadNum());
    pool.prestartAllCoreThreads();
  }

  @Override
  public void put(List<Integer> redisIds, String cacheKey, long fieldKey, final byte[][] chunks) throws ECFileCacheException {

    long dataLength = checkDataAndGetLength(chunks);
    List<DecoratedJedisPool> jedisPools = getJedisPools(redisIds);

    CompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(pool);
    List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

    int failCount = 0;
    for (int i = 0; i < jedisPools.size(); ++i) {
      DecoratedJedisPool jedis = jedisPools.get(i);
      if (jedis != null) {
        final String key = cacheKey + SEP + i;
        final String field = fieldKey + SEP + dataLength;
        final byte[] data = chunks[i];
        RedisPutChunk redisPutChunk = new RedisPutChunk(jedis, key, field, data);

        if (!pool.isShutdown()) {
          Future<Integer> future = completionService.submit(redisPutChunk);
          futures.add(future);
        }
      } else {
        failCount++;
      }
    }
    checkRedisResult(completionService, futures, failCount);
  }

  @Override
  public List<Pair<byte[][], int[]>> get(List<Integer> redisIds, String cacheKey) throws ECFileCacheException {

    List<DecoratedJedisPool> jedisPools = getJedisPools(redisIds);

    @SuppressWarnings("unchecked")
    Map<byte[], byte[]>[] redisDataList = new Map[redisIds.size()];

    CompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(pool);
    List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

    int failCount = 0;
    for (int i = 0; i < jedisPools.size(); ++i) {
      DecoratedJedisPool jedis = jedisPools.get(i);
      if (jedis != null) {
        final String key = cacheKey + SEP + i;
        RedisGetAll redisGetAll = new RedisGetAll(jedis, key, redisDataList, i);

        if (!pool.isShutdown()) {
          Future<Integer> future = completionService.submit(redisGetAll);
          futures.add(future);
        }
      } else {
        failCount++;
      }
    }
    checkRedisResult(completionService, futures, failCount);

    return convert(redisDataList);
  }

  @Override
  public Pair<byte[][], int[]> getChunk(String cacheKey, long chunkPos, int chunkSize, List<Integer> redisIds) throws
                                                         ECFileCacheException {

    List<DecoratedJedisPool> jedisPools = getJedisPools(redisIds);

    byte[][] redisDataArray = new byte[jedisPools.size()][];

    CompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(pool);
    List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

    String field = chunkPos + SEP + chunkSize;

    int failCount = 0;
    for (int i = 0; i < jedisPools.size(); ++i) {
      DecoratedJedisPool jedis = jedisPools.get(i);
      if (jedis != null) {
        String key = cacheKey + SEP + i;
        RedisGetChunk redisGetChunk = new RedisGetChunk(jedis, key, field, redisDataArray, i);

        if (!pool.isShutdown()) {
          Future<Integer> future = completionService.submit(redisGetChunk);
          futures.add(future);
        }
      } else {
        failCount++;
      }
    }
    checkRedisResult(completionService, futures, failCount);

    return convertChunk(redisDataArray, chunkSize);
  }

  @Override
  public Map<Long, Integer> getChunkPosAndSize(List<Integer> redisIds, String cacheKey) throws ECFileCacheException {

    List<DecoratedJedisPool> jedisPools = getJedisPools(redisIds);
    @SuppressWarnings("unchecked")
    Set<byte[]>[] redisFields = new Set[jedisPools.size()];

    CompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(pool);
    List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

    int failCount = 0;
    for (int i = 0; i < jedisPools.size(); ++i) {

      DecoratedJedisPool jedis = jedisPools.get(i);
      if (jedis != null) {
        final String key = cacheKey + SEP + Integer.toString(i);
        RedisHKeys redisHKeys = new RedisHKeys(jedis, key, redisFields, i);

        if (!pool.isShutdown()) {
          Future<Integer> future = completionService.submit(redisHKeys);
          futures.add(future);
        }
      } else {
        failCount++;
      }
    }
    checkRedisResult(completionService, futures, failCount);

    return convertChunkPosAndSize(redisFields);
  }

  @Override
  public void delete(String cacheKey, List<Integer> redisIds) throws ECFileCacheException {

    List<DecoratedJedisPool> jedisPools = getJedisPools(redisIds);
    List<RedisDelete> redisDeleteList = new ArrayList<RedisDelete>();
    for (int i = 0; i < jedisPools.size(); ++i) {

      String key = cacheKey + SEP + Integer.toString(i);
      RedisDelete redisDelete = new RedisDelete(jedisPools.get(i), key);
      redisDeleteList.add(redisDelete);
    }
    try {
      pool.invokeAll(redisDeleteList, checkResultTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.warn(String.format("delete key[%s] interrupted", cacheKey), e);
    }
  }

  private void checkRedisResult(CompletionService<Integer> completionService, List<Future<Integer>> futures, int failCount) throws ECFileCacheException {
    long timeoutMs = checkResultTimeoutMs;
    long lastTime = System.currentTimeMillis();

    for (int i = 0, taskNum = futures.size(); i < taskNum; ++i) {
      boolean success = false;
      try {
        Future<Integer> task = completionService.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (task != null && task.get() == 0) {
          success = true;
        } else {
          long timeoutMsNow = timeoutMs - (System.currentTimeMillis() - lastTime);
          LOGGER.warn("communicate with redis timeout or failed. remain timeout[{}ms]", timeoutMsNow);
        }
      } catch (CancellationException e) {
        LOGGER.error("task cancelled", e);
      } catch (InterruptedException e) {
        LOGGER.error("interrupted exception", e);
      } catch (ExecutionException e) {
        LOGGER.error("execution exception", e);
      }

      long now = System.currentTimeMillis();
      timeoutMs -= now - lastTime;
      lastTime = now;

      if (!success) {
        failCount++;
      }

      if (failCount > ECodec.CODING_BLOCK_NUM) {
        for (Future<Integer> future : futures) {
          future.cancel(true);
        }
        checkFail(failCount, CHECK_REDIS_RESULT, "NIL");
      }
    }
  }

}
