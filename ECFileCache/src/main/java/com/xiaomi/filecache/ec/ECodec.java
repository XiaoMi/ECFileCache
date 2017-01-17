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
import com.xiaomi.filecache.ec.utils.DataUtil;
import com.xiaomi.infra.ec.ErasureCodec;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class ECodec {
  private static final Logger LOGGER = LoggerFactory.getLogger(ECodec.class.getName());

  private final ErasureCodec codec;
  public static final int WORD_SIZE = 8;
  public static final int PACKET_SIZE = 1024;
  public static final byte VERSION = (byte) 1;

  public static final int DATA_BLOCK_NUM = 8;
  public static final int CODING_BLOCK_NUM = 3;
  public static final int EC_BLOCK_NUM = DATA_BLOCK_NUM + CODING_BLOCK_NUM;
  public static final int MIN_DATA_LEN = WORD_SIZE * PACKET_SIZE * DATA_BLOCK_NUM;

  private ECodec() {
    codec = new ErasureCodec.Builder(ErasureCodec.Algorithm.Cauchy_Reed_Solomon)
        .dataBlockNum(DATA_BLOCK_NUM)
        .codingBlockNum(CODING_BLOCK_NUM)
        .wordSize(WORD_SIZE)
        .packetSize(PACKET_SIZE)
        .good(true)
        .build();
  }

  private static class ECodecHolder {
    static final ECodec INSTANCE = new ECodec();
  }

  public static ECodec getInstance() {
    return ECodecHolder.INSTANCE;
  }



  public byte[][] encode(byte[] data) throws ECFileCacheException {

    if (ArrayUtils.isEmpty(data)) {
      String verbose = "empty data";
      LOGGER.error(verbose);
      throw new ECFileCacheException(verbose);
    }

    if (data.length % MIN_DATA_LEN != 0) {
      String verbose = String.format("invalid data, length of data must be multiple of %d", MIN_DATA_LEN);
      LOGGER.error(verbose);
      throw new ECFileCacheException(verbose);
    }

    byte[][] dataBlock = DataUtil.arrayToArray2D(data, DATA_BLOCK_NUM);
    Validate.isTrue(DATA_BLOCK_NUM == dataBlock.length);

    byte[][] codingBlock;

    synchronized (ECodec.class) {
      codingBlock = codec.encode(dataBlock);
    }

    return DataUtil.concatArray2D(dataBlock, codingBlock);
  }

  public byte[] decode(byte[][] data, int[] erasures) throws ECFileCacheException {

    if (data.length != DATA_BLOCK_NUM + CODING_BLOCK_NUM) {
      String verbose = String.format("block num must be equal to %d.", DATA_BLOCK_NUM + CODING_BLOCK_NUM);
      LOGGER.error(verbose);
      throw new ECFileCacheException(verbose);
    }

    byte[][] dataBlock = DataUtil.getPartArray2D(data, DATA_BLOCK_NUM);

    if (checkErasuresAndGetMin(erasures) < DATA_BLOCK_NUM) {
      byte[][] codingBlock = DataUtil.getPartArray2D(data, -CODING_BLOCK_NUM);

      codec.decode(erasures, dataBlock, codingBlock);
    }
    return DataUtil.array2DToArray(dataBlock);
  }

  private int checkErasuresAndGetMin(int[] erasures) throws ECFileCacheException {
    if (ArrayUtils.isEmpty(erasures)) {
      throw new ECFileCacheException("empty erasures array");
    }

    if (erasures.length > CODING_BLOCK_NUM) {
      String verbose = String.format("can not decode data with [%d] blocks erased.", erasures.length);
      LOGGER.warn(verbose);
      throw new ECFileCacheException(verbose);
    }

    int min = Integer.MAX_VALUE;
    Set<Integer> set = new HashSet<Integer>();
    for (final int erasure : erasures) {
      if (erasure < -1 || erasure >= EC_BLOCK_NUM || set.contains(erasure)) {
        throw new ECFileCacheException("invalid erasures array");
      }
      set.add(erasure);

      if (erasure < min) {
        min = erasure;
      }
    }

    return min;
  }
}
