/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.xiaomi.infra.ec;

import com.xiaomi.infra.ec.rs.CauchyReedSolomonCodec;
import com.xiaomi.infra.ec.rs.ReedSolomonCodec;

/**
 * ErasureCodec defines methods for users to encode given data block to
 * erasure codes block, and to decode data block from given data and erasure
 * codes blocks.
 */
public class ErasureCodec implements CodecInterface {

  /**
   * Currently supported coding algorithms.
   */
  public enum Algorithm {
    Reed_Solomon,
    Cauchy_Reed_Solomon;
  }

  /**
   * Builder class for ErasureCodec.
   */
  public static class Builder {
    private Algorithm algorithm;
    private int dataBlockNum;
    private int codingBlockNum;
    private int wordSize;
    private int packetSize;
    private boolean good;

    public Builder(Algorithm algorithm) {
      this.algorithm = algorithm;
    }

    public ErasureCodec build() {
      CodecInterface codec = null;
      switch (algorithm) {
        case Reed_Solomon:
          codec = new ReedSolomonCodec(dataBlockNum, codingBlockNum, wordSize);
          break;
        case Cauchy_Reed_Solomon:
          codec = new CauchyReedSolomonCodec(dataBlockNum, codingBlockNum,
              wordSize, packetSize, good);
          break;
        default:
          throw new IllegalArgumentException("Algorithm is not supported: "
              + algorithm);
      }
      return new ErasureCodec(codec);
    }

    public Builder dataBlockNum(int dataBlockNum) {
      this.dataBlockNum = dataBlockNum;
      return this;
    }

    public Builder codingBlockNum(int codingBlockNum) {
      this.codingBlockNum = codingBlockNum;
      return this;
    }

    public Builder wordSize(int wordSize) {
      this.wordSize = wordSize;
      return this;
    }

    public Builder packetSize(int packetSize) {
      this.packetSize = packetSize;
      return this;
    }

    public Builder good(boolean good) {
      this.good = good;
      return this;
    }
  }

  private CodecInterface wrappedCodec;

  private ErasureCodec(CodecInterface codec) {
    wrappedCodec = codec;
  }

  /** {@inheritDoc} */
  @Override
  public byte[][] encode(byte[][] data) {
    return wrappedCodec.encode(data);
  }

  /** {@inheritDoc} */
  @Override
  public void decode(int[] erasures, byte[][] data, byte[][] coding) {
    wrappedCodec.decode(erasures, data, coding);
  }
}
