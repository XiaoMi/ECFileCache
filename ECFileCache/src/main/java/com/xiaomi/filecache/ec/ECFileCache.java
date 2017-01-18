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
package com.xiaomi.filecache.ec;


import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import com.xiaomi.filecache.ec.io.ECFileCacheInputStream;
import com.xiaomi.filecache.ec.redis.RedisAccessBase;
import com.xiaomi.filecache.ec.utils.Pair;
import com.xiaomi.filecache.ec.utils.SerializationHelper;
import com.xiaomi.filecache.ec.zk.ZKChildMonitor;
import com.xiaomi.filecache.thrift.FileCacheKey;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class ECFileCache {

  private ECodec eCodec = ECodec.getInstance();
  private ZKChildMonitor monitor;

  private static final Logger LOGGER = LoggerFactory.getLogger(ECFileCache.class.getName());

  public ECFileCache(short clusterId, short partitionId) {
    monitor = ZKChildMonitor.getInstance(clusterId, partitionId);
  }

  /**
   * Create file cache key
   *
   * @param size file size
   * @return fileCacheId. uniq file cache key
   * @throws java.security.InvalidParameterException
   */
  public String createFileCacheKey(Integer size) throws ECFileCacheException {

    int clusterSize = monitor.get().getKeyedPool().size();

    String cacheKey = UUID.randomUUID().toString().replace("-", "");
    int offset = genDeviceOffset(clusterSize);

    FileCacheKey fileCacheKey = new FileCacheKey(cacheKey,
        ECodec.VERSION,
        (short) clusterSize,
        (short) offset);

    if (size != null) {
      fileCacheKey.setFileSize(size);
    }

    byte[] bytes = SerializationHelper.toBytes(fileCacheKey);
    return Base64.encodeBase64URLSafeString(bytes);
  }

  /**
   * Cache file
   *
   * @param fileCacheKeyStr file cache key
   * @param chunkPos chunk offset
   * @param inputStream chunk content
   * @param crc32 crc32 of chunk
   * @return start pos of next chunk
   */

  public long putFile(final String fileCacheKeyStr, long chunkPos, final InputStream inputStream, final long crc32)
      throws ECFileCacheException {

    long nextChunkPos;
    try {
      FileCacheKey fileCacheKey = SerializationHelper.toThriftObject(FileCacheKey.class, Base64.decodeBase64(fileCacheKeyStr));
      Validate.isTrue(fileCacheKey.getVersion() == ECodec.VERSION);

      byte[] data = IOUtils.toByteArray(inputStream);
      nextChunkPos = chunkPos + data.length;

      if (nextChunkPos >= fileCacheKey.getFileSize() && data.length % ECodec.MIN_DATA_LEN != 0) {
        // padding the last chunk if need
        int paddingLength = data.length - (data.length % ECodec.MIN_DATA_LEN) + ECodec.MIN_DATA_LEN;

        String verbose = String.format("padding data, origin length [%d], padding length [%d]",
            data.length, paddingLength);
        LOGGER.info(verbose);

        data = Arrays.copyOf(data, paddingLength);
      }

      byte[][] dataAndCoding = eCodec.encode(data);

      List<Integer> redisIds = getRedisIds(fileCacheKey);

      monitor.get().put(redisIds, fileCacheKey.getUuid(), chunkPos, dataAndCoding);

    } catch (IOException e) {
      String verbose = "read inputStream exception";
      LOGGER.error(verbose, e);
      throw new ECFileCacheException(verbose, e);
    } finally {
      IOUtils.closeQuietly(inputStream);
    }
    return nextChunkPos;
  }

  /**
   * Get the whole file
   *
   * @param fileCacheKeyStr file cache key
   * @return        entire file
   */
  public byte[] getFile(final String fileCacheKeyStr) throws ECFileCacheException {
    FileCacheKey fileCacheKey = SerializationHelper.toThriftObject(FileCacheKey.class, Base64.decodeBase64(fileCacheKeyStr));
    Validate.isTrue(fileCacheKey.getVersion() == ECodec.VERSION);

    List<Integer> redisIds = getRedisIds(fileCacheKey);

    List<byte[]> chunkList = new ArrayList<byte[]>();

    List<Pair<byte[][], int[]>> chunks;
    chunks = monitor.get().get(redisIds, fileCacheKey.getUuid());

    for (Pair<byte[][], int[]> pair : chunks) {
      byte[][] chunk = pair.getFirst();
      int[] erasures = pair.getSecond();

      if (erasures.length > ECodec.CODING_BLOCK_NUM) {
        String verbose = String.format("can not decode chunk, erasures data num[%d] > CODING_BLOCK_NUM[%d]",
            erasures.length, ECodec.CODING_BLOCK_NUM);
        LOGGER.error(verbose);
        throw new ECFileCacheException(verbose);
      }

      byte[] buffer = eCodec.decode(chunk, erasures);

      chunkList.add(buffer);
    }

    byte[] data = null;
    for (byte[] buffer : chunkList) {
      data = ArrayUtils.addAll(data, buffer);
    }

    if (data == null) {
      return null;
    }

    int fileSize = (int) fileCacheKey.getFileSize();
    if (data.length > fileSize) {
      data = Arrays.copyOf(data, fileSize);
    }

    return data;
  }

  /**
   * Get file as an input stream
   *
   * @param fileCacheKeyStr file cache key
   * @return file stream
   */
  public InputStream asInputStream(final String fileCacheKeyStr) throws ECFileCacheException {
    return asInputStream(fileCacheKeyStr, null);
  }

  /**
   * Get file as an input stream.
   * Append last chunk data to cached file
   *
   * @param fileCacheKeyStr file cache key
   * @param endChunkStream last chunk of file
   * @return file stream
   */
  public InputStream asInputStream(final String fileCacheKeyStr, InputStream endChunkStream) throws ECFileCacheException {
    FileCacheKey fileCacheKey = SerializationHelper.toThriftObject(FileCacheKey.class, Base64.decodeBase64(fileCacheKeyStr));
    Validate.isTrue(fileCacheKey.getVersion() == ECodec.VERSION);

    List<Integer> redisIds = getRedisIds(fileCacheKey);

    Map<Long, Integer> chunkPosAndSize = monitor.get().getChunkPosAndSize(redisIds, fileCacheKey.getUuid());
    return new ECFileCacheInputStream(fileCacheKey, chunkPosAndSize, monitor.get(), redisIds, endChunkStream);
  }

  /**
   * Delete cached file
   *
   * @param fileCacheKeyStr file cache key
   */
  public void deleteFile(final String fileCacheKeyStr) throws ECFileCacheException {
    FileCacheKey fileCacheKey = SerializationHelper.toThriftObject(FileCacheKey.class, Base64.decodeBase64(fileCacheKeyStr));
    Validate.isTrue(fileCacheKey.getVersion() == ECodec.VERSION);

    List<Integer> redisIds = getRedisIds(fileCacheKey);
    monitor.get().delete(redisIds, fileCacheKey.getUuid());
  }

  /**
   * Cache key-value data
   *
   * @param keyStr cache key
   * @param data cache value
   */
  public void putExtraInfo(final String keyStr, byte[] data) {

    int redisId = genRedisId(keyStr);
    monitor.get().putInfo(redisId, keyStr, data);
  }

  /**
   * Get key-value data
   *
   * @param keyStr cache key
   * @return cache value
   */
  public byte[] getExtraInfo(final String keyStr) {
    int redisId = genRedisId(keyStr);
    return monitor.get().getInfo(redisId, keyStr);
  }

  private int genDeviceOffset(int clusterSize) throws ECFileCacheException {
    List<Integer> deviceIds = new ArrayList<Integer>(monitor.get().getKeyedPool().keySet());

    Random random = new Random();
    int retry = 0;
    do {
      int offset = random.nextInt(clusterSize);

      int i;
      int count = 0;
      for (i = 0; i < ECodec.EC_BLOCK_NUM; ++i) {
        if (deviceIds.contains((offset + i) % clusterSize)) {
          ++count;
        }
      }

      if (count >= ECodec.EC_BLOCK_NUM) {
        return offset;
      } else if (retry >= Config.getInstance().getTolerateErasedDeviceAfterRetry() && count >= ECodec.DATA_BLOCK_NUM) {
        return offset;
      }

      if (++retry >= Config.getInstance().getSelectOffsetMaxRetry()) {
        String verbose = "can not allocate offset id for cacheKey";
        LOGGER.error(verbose);
        throw new ECFileCacheException(verbose);
      }
    } while (true);
  }

  private List<Integer> getRedisIds(final FileCacheKey fileCacheKey) throws ECFileCacheException {
    int offset = fileCacheKey.getDeviceOffset();
    int clusterSize = fileCacheKey.getDeviceClusterSize();
    if (offset < 0 || offset >= clusterSize) {
      String verbose = String.format("invalid offset id [%d]", offset);
      LOGGER.error(verbose);
      throw new ECFileCacheException(verbose);
    }

    List<Integer> redisIds = new ArrayList<Integer>();
    for (int i = 0, n = ECodec.EC_BLOCK_NUM; i < n; ++i) {
      redisIds.add((offset + i) % clusterSize);
    }

    return redisIds;
  }

  private int genRedisId(String str) {
    int hash = str.hashCode();
    int key = (hash == Integer.MIN_VALUE) ? 0 : Math.abs(hash);
    return key % monitor.get().getKeyedPool().size();
  }

  public RedisAccessBase getRedisAccess() {
    return monitor.get();
  }
}
