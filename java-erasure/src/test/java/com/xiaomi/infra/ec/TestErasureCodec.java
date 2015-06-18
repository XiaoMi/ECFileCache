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

import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.infra.ec.ErasureCodec.Algorithm;
import com.xiaomi.infra.ec.ErasureCodec.Builder;

public class TestErasureCodec {

  @Test
  public void TestReedSolomonCodec() {
    ErasureCodec codec = new Builder(Algorithm.Reed_Solomon)
        .dataBlockNum(6)
        .codingBlockNum(3)
        .wordSize(8)
        .build();
    runTest(codec, 6, 3, 32, true);
  }

  @Test
  public void TestCauchyReedSolomonCodec() {
    ErasureCodec codec = new Builder(Algorithm.Cauchy_Reed_Solomon)
        .dataBlockNum(6)
        .codingBlockNum(3)
        .wordSize(4)
        .packetSize(8)
        .build();
    runTest(codec, 6, 3, 32, true);
  }

  @Test
  public void TestGoodCauchyReedSolomonCodec() {
    ErasureCodec codec = new Builder(Algorithm.Cauchy_Reed_Solomon)
        .dataBlockNum(6)
        .codingBlockNum(3)
        .wordSize(4)
        .packetSize(8)
        .good(true)
        .build();
    runTest(codec, 6, 3, 32, true);
  }

  @Test
  public void testWithZeroLines() {
    ErasureCodec codec = new Builder(Algorithm.Reed_Solomon)
        .dataBlockNum(6)
        .codingBlockNum(3)
        .wordSize(8)
        .build();
    byte[][] data = new byte[6][8];
    byte[][] coding = new byte[3][8];

    data[0] = new byte[]{-57, 0, 103, 111, -20, -46, -78, 85};
    data[1] = new byte[]{27, 80, -92, 33, -80, 17, -125, 72};
    data[2] = new byte[]{111, -97, 33, 75, 116, 80, -109, -127};

    coding[1] = new byte[]{35, -125, -20, -71, 40, 46, 40, 39};
    CodecUtils.printMatrix(data, true);
    CodecUtils.printMatrix(coding, true);

    int[] erasures = new int[]{3, 6};
    codec.decode(erasures, data, coding);
    CodecUtils.printMatrix(data, true);
    CodecUtils.printMatrix(coding, true);

    /*
    Random random = new Random();
    for (int i = 0; i < data.length; ++i) {
      if (i < 2) {
        random.nextBytes(data[i]);
      } else {
        Arrays.fill(data[i], 0, data[i].length, (byte)0);
      }
    }
    CodecUtils.printMatrix(data, true);

    byte[][] coding = codec.encode(data);
    CodecUtils.printMatrix(coding, true);

    int[] erasures = {1, 6};
    Arrays.fill(data[1], 0, data[1].length, (byte)0);
    Arrays.fill(coding[0], 0, coding[0].length, (byte)0);
    codec.decode(erasures, data, coding);
    CodecUtils.printMatrix(data, true);
    CodecUtils.printMatrix(coding, true);
    */
  }

  private void runTest(CodecInterface codec, int k, int m, int size,
      boolean printMatrix) {
    long t1 = System.currentTimeMillis();
    Random random = new Random();
    // Generate data
    byte[][] data = new byte[k][size];
    byte[][] copiedData = new byte[k][size];
    for (int r = 0; r < data.length; ++r) {
      random.nextBytes(data[r]);
      System.arraycopy(data[r], 0, copiedData[r], 0, data[r].length);
    }
    System.out.println("Original data matrix:");
    CodecUtils.printMatrix(data, printMatrix);
    long t2 = System.currentTimeMillis();

    // Encode the data
    byte[][] coding = codec.encode(data);
    byte[][] copiedCoding = new byte[coding.length][coding[0].length];
    for (int r = 0; r < coding.length; ++r) {
      System.arraycopy(coding[r], 0, copiedCoding[r], 0, coding[r].length);
    }
    System.out.println("Original coding matrix:");
    CodecUtils.printMatrix(coding, printMatrix);
    long t3 = System.currentTimeMillis();

    // Erasure m random blocks
    int erasures[] = new int[m];
    int erasured[] = new int[k + m];
    for (int i = 0; i < m;) {
      int randomNum = random.nextInt(k + m);
      erasures[i] = randomNum;

      if (erasured[erasures[i]] == 0) {
        erasured[erasures[i]] = 1;

        if (erasures[i] < k) {
          Arrays.fill(data[erasures[i]], 0, data[0].length, (byte)0);
        } else {
          Arrays.fill(coding[erasures[i] - k], 0, data[0].length, (byte)0);
        }
        ++i;
      }
    }
    System.out.println("Erasures matrix:");
    CodecUtils.printMatrix(erasures, 1, erasures.length, printMatrix);
    System.out.println("Erasured data matrix:");
    CodecUtils.printMatrix(data, printMatrix);
    System.out.println("Erasured coding matrix:");
    CodecUtils.printMatrix(coding, printMatrix);
    long t4 = System.currentTimeMillis();

    // Decode data
    codec.decode(erasures, data, coding);
    System.out.println("Decoded data matrix:");
    CodecUtils.printMatrix(data, printMatrix);
    System.out.println("Decoded coding matrix:");
    CodecUtils.printMatrix(coding, printMatrix);
    long t5 = System.currentTimeMillis();

    System.out.println("====Time Stats====");
    System.out.printf("Generate data:\t%d\n", (t2 - t1));
    System.out.printf("Encode data:\t%d\n", (t3 - t2));
    System.out.printf("Erasure data:\t%d\n", (t4 - t3));
    System.out.printf("Decode data:\t%d\n\n", (t5 - t4));

    // Check result
    Assert.assertArrayEquals(copiedData, data);
    Assert.assertArrayEquals(copiedCoding, coding);
  }
}
