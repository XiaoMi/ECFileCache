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
package com.xiaomi.infra.ec.rs;

import com.google.common.base.Preconditions;
import com.sun.jna.Pointer;

import com.xiaomi.infra.ec.CodecInterface;
import com.xiaomi.infra.ec.CodecUtils;
import com.xiaomi.infra.ec.JerasureLibrary;

/**
 * Reed Solomon erasure codec, implemented with Cauchy matrix.
 */
public class CauchyReedSolomonCodec implements CodecInterface {

  private int dataBlockNum;
  private int codingBlockNum;
  private int wordSize;
  private int packetSize;
  private int[] cauchyBitMatrix;
  private boolean good;
  private Pointer[] schedulePtrs;

  public CauchyReedSolomonCodec(int dataBlockNum, int codingBlockNum,
      int wordSize, int packetSize, boolean good) {
    Preconditions.checkArgument(dataBlockNum > 0);
    Preconditions.checkArgument(codingBlockNum > 0);
    Preconditions.checkArgument(packetSize > 0);
    Preconditions.checkArgument((dataBlockNum + codingBlockNum) < (1<<wordSize),
        "dataBlocksNum + codingBlocksNum is larger than 2^wordSize");
    Preconditions.checkArgument(packetSize % 8 == 0,
        "packetSize must be multiple of 8");

    this.dataBlockNum = dataBlockNum;
    this.codingBlockNum = codingBlockNum;
    this.wordSize = wordSize;
    this.packetSize = packetSize;
    this.good = good;

    int[] matrix;
    if (good) {
      matrix = createGoodCauchyMatrix(dataBlockNum,
          codingBlockNum, wordSize);
    } else {
      matrix = createCauchyMatrix(dataBlockNum,
          codingBlockNum, wordSize);
    }
    this.cauchyBitMatrix = convertToBitMatrix(dataBlockNum,
        codingBlockNum, wordSize, matrix);
    this.schedulePtrs = JerasureLibrary.INSTANCE
        .jerasure_smart_bitmatrix_to_schedule(this.dataBlockNum,
            this.codingBlockNum, this.wordSize, this.cauchyBitMatrix);
  }

  /** {@inheritDoc} */
  @Override
  public byte[][] encode(byte[][] data) {
    Preconditions.checkArgument(data.length > 0);
    Preconditions.checkArgument(data[0].length % (wordSize * packetSize) == 0,
        "data length must be multiple of wordSize * packetSize");

    Pointer[] dataPtrs = CodecUtils.toPointerArray(data);
    int size = data[0].length;
    byte[][] coding = new byte[codingBlockNum][size];
    Pointer[] codingPtrs = CodecUtils.toPointerArray(coding);

    JerasureLibrary.INSTANCE.jerasure_schedule_encode(dataBlockNum,
        codingBlockNum, wordSize, schedulePtrs, dataPtrs, codingPtrs,
        size, packetSize);
    CodecUtils.toByteArray(codingPtrs, coding);
    return coding;
  }

  /** {@inheritDoc} */
  @Override
  public void decode(int[] erasures, byte[][]data, byte[][] coding) {
    Preconditions.checkArgument(data.length > 0);

    Pointer[] dataPtrs = CodecUtils.toPointerArray(data);
    Pointer[] codingPtrs = CodecUtils.toPointerArray(coding);
    erasures = CodecUtils.adjustErasures(erasures);
    int size = data[0].length;

    int ret = JerasureLibrary.INSTANCE.jerasure_schedule_decode_lazy(
        dataBlockNum, codingBlockNum, wordSize, cauchyBitMatrix, erasures,
        dataPtrs, codingPtrs, size, packetSize, 1);
    if (ret == 0) {
      CodecUtils.copyBackDecoded(dataPtrs, codingPtrs, erasures, data, coding);
    } else {
      throw new RuntimeException("Decode fail, return_code=" + ret);
    }
  }

  /**
   * Creates a Cauchy matrix over GF(2^w).
   *
   * @param k The column number
   * @param m The row number
   * @param w The word size, used to define the finite field
   * @return The generated Cauchy matrix
   */
  int[] createCauchyMatrix(int k, int m , int w) {
    Pointer matrix = JerasureLibrary.INSTANCE
        .cauchy_original_coding_matrix(k, m, w);
    return matrix.getIntArray(0, k * m);
  }

  /**
   * Creates a optimized Cauchy matrix over GF(2^w).
   *
   * @param k The column number
   * @param m The row number
   * @param w The word size, used to define the finite field
   * @return The generated Cauchy matrix
   */
  int[] createGoodCauchyMatrix(int k, int m , int w) {
    Pointer matrix = JerasureLibrary.INSTANCE
        .cauchy_good_general_coding_matrix(k, m, w);
    return matrix.getIntArray(0, k * m);
  }

  /**
   * Converts the Cauchy matrix to a bit matrix over GF(2^w).
   *
   * @param k The column number
   * @param m The row number
   * @param w The word size, used to define the finite field
   * @param matrix The cauchy matrix
   * @return The converted bit matrix
   */
  int[] convertToBitMatrix(int k, int m, int w, int[] matrix) {
    Pointer bit_matrix = JerasureLibrary.INSTANCE.jerasure_matrix_to_bitmatrix(
        k, m, w, matrix);
    return bit_matrix.getIntArray(0, k * w * m * w);
  }
}
