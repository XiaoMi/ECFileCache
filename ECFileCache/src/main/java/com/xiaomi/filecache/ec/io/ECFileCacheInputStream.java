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
package com.xiaomi.filecache.ec.io;

import com.xiaomi.filecache.ec.ECodec;
import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import com.xiaomi.filecache.ec.redis.RedisAccessBase;
import com.xiaomi.filecache.ec.utils.Pair;
import com.xiaomi.filecache.thrift.FileCacheKey;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ECFileCacheInputStream extends InputStream {

  private final ECodec eCodec = ECodec.getInstance();
  private final RedisAccessBase redisAccess;

  private volatile byte[] buf;
  private int pos;
  private int count;
  private boolean isClosed = false;

  private final String key;
  private final int fileSize;
  private final Map<Long, Integer> chunkPosAndSize;
  private final List<Integer> redisIds;

  private int nextChunkPos = 0;
  private final InputStream endChunkStream;

  private static final Logger LOGGER = LoggerFactory.getLogger(ECFileCacheInputStream.class.getName());

  /**
   * Constructs a input stream to read cached data
   *
   * @param cacheKey the key of cached data
   * @param chunkPosAndSize chunk info of cached data
   * @param redisAccess object to read data from redis
   * @param redisIds Redis list that saved the data
   * @param stream the input stream of last chunk data
   */
  public ECFileCacheInputStream(FileCacheKey cacheKey, Map<Long, Integer> chunkPosAndSize, RedisAccessBase redisAccess,
                  List<Integer> redisIds, InputStream stream) {
    this.key = cacheKey.getUuid();
    this.fileSize = (int) cacheKey.getFileSize();
    this.chunkPosAndSize = chunkPosAndSize;
    this.redisAccess = redisAccess;
    this.redisIds = redisIds;
    this.endChunkStream = stream;
  }

  /**
   * Reads the next byte of data from the input stream
   *
   * @return     the next byte of data, or
   *             <code>-1</code> if the end of the stream is reached
   * @throws IOException
   */
  @Override
  public int read() throws IOException {
    if (pos >= count) {
      fill();
      if (pos >= count) {
        return -1;
      }
    }
    checkIfClosed();
    return buf[pos++] & 0xFF;
  }

  private void fill() throws IOException {
    checkIfClosed();
    count = pos = 0;
    byte[] buffer;

    try {
      buffer = getChunk();
    } catch (ECFileCacheException e) {
      String verbos = "get chunk data from redis failed";
      LOGGER.error(verbos, e);
      throw new IOException(verbos, e);
    }

    if (!ArrayUtils.isEmpty(buffer)) {
      buf = buffer;
      count = buffer.length;
    }
  }

  private void checkIfClosed() throws IOException {
    if (isClosed) {
      throw new IOException("Stream closed");
    }
  }

  /**
   * Reads up to <code>len</code> bytes of data from the input stream into
   * an array of bytes.  An attempt is made to read as many as
   * <code>len</code> bytes, but a smaller number may be read.
   * The number of bytes actually read is returned as an integer.
   *
   * @param b     the buffer into which the data is read.
   * @param off   the start offset in array <code>b</code> at which the data is written.
   * @param len   the maximum number of bytes to read.
   * @return     the total number of bytes read into the buffer, or
   *             <code>-1</code> if the end of the stream has been reached.
   * @throws IOException
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkIfClosed();
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int n = 0;
    while (true) {
      int nread = readRedisIfNeed(b, off + n, len - n);
      if (nread <= 0) {
        return (n == 0) ? nread : n;
      }
      n += nread;
      if (n >= len) {
        return n;
      }
      if (available() <= 0) {
        return n;
      }
    }
  }

  private int readRedisIfNeed(byte[] b, int off, int len) throws IOException {
    checkIfClosed();
    int avail = count - pos;
    if (avail <= 0) {
      fill();
      avail = count - pos;
      if (avail <= 0) {
        return -1;
      }
    }
    int cnt = (avail < len) ? avail : len;
    System.arraycopy(buf, pos, b, off, cnt);
    pos += cnt;
    return cnt;
  }

  private byte[] getChunk() throws ECFileCacheException {

    if(nextChunkPos >= fileSize){
      return null;
    }

    byte[] buffer;
    long chunkPos = nextChunkPos;
    Integer size = chunkPosAndSize.get(chunkPos);
    if (size == null) {
      if (endChunkStream == null) {
        return null;
      }

      try {
        buffer = IOUtils.toByteArray(endChunkStream);
      } catch (IOException e) {
        String verbose = "read end chunk stream data exception";
        LOGGER.error(verbose, e);
        throw new ECFileCacheException(verbose, e);
      }
    } else {
      buffer = getDataFromRedis(chunkPos, size);
    }

    nextChunkPos += buffer.length;
    if (nextChunkPos > fileSize) {
      buffer = Arrays.copyOf(buffer, fileSize - (int) chunkPos);

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(String.format("Trim padding. padded length [%d], available length [%d]",
            buffer.length, fileSize - (int) chunkPos));
      }
    }

    return buffer;
  }

  private byte[] getDataFromRedis(long chunkPos, int size) throws ECFileCacheException {
    Pair<byte[][], int[]> pair;
    pair = redisAccess.getChunk(redisIds, key, chunkPos, size);

    if (pair == null) {
      return null;
    }

    byte[][] chunk = pair.getFirst();
    int[] erasures = pair.getSecond();

    if (erasures.length > ECodec.CODING_BLOCK_NUM) {
      String verbose = String.format("can not decode chunk, erasures data num[%d] > CODING_BLOCK_NUM[%d]",
          erasures.length, ECodec.CODING_BLOCK_NUM);
      LOGGER.error(verbose);
      throw new ECFileCacheException(verbose);
    }
    return eCodec.decode(chunk, erasures);
  }

  /**
   * Returns the number of bytes that can be read from this input stream
   *
   * @return the number of bytes that can be read, or
   *         <code>0</code> if the end of the stream has been reached.
   * @throws IOException
   */
  @Override
  public int available() throws IOException {
    return fileSize - nextChunkPos + (count - pos);
  }

  /**
   * close this input stream
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    isClosed = true;
  }
}
