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
package com.xiaomi.filecache.ec.utils;

import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import org.apache.commons.lang3.ArrayUtils;

public class DataUtil {

  // 2D array operation

  /**
   * Convert 1D array to 2D array
   *
   * @param b 1D array
   * @param k row number of 2D array
   * @return k x (b.length/k) 2D array
   * @throws ECFileCacheException
   */
  public static byte[][] arrayToArray2D(byte[] b, int k) throws ECFileCacheException {
    if (k == 0) {
      throw new ECFileCacheException("can not convert to zero size array");
    }
    if (ArrayUtils.isEmpty(b) || b.length % k != 0) {
      throw new ECFileCacheException("invalid data array size");
    }
    int length = b.length / k;
    byte[][] bb = new byte[k][length];
    for (int i = 0; i < k; ++i) {
      System.arraycopy(b, i * length, bb[i], 0, length);
    }
    return bb;
  }

  /**
   * Convert 2D array to 1D array
   *
   * @param bb 2D array
   * @return 1D array
   * @throws ECFileCacheException
   */
  public static byte[] array2DToArray(byte[][] bb) throws ECFileCacheException {
    if (ArrayUtils.isEmpty(bb)) {
      String verbose = "empty data";
      throw new ECFileCacheException(verbose);
    }

    int k = bb.length;
    int length = bb[0].length;
    int totalLength = k * length;

    if(totalLength == 0){
      return new byte[0];
    }

    byte[] b = new byte[totalLength];
    for (int i = 0; i < k; ++i) {
      System.arraycopy(bb[i], 0, b, i * length, length);
    }
    return b;
  }

  /**
   * Concat two 2D array
   *
   * @param a first 2D array
   * @param b second 2D array
   * @return 2D array like
   *                 +---+
   *                 | a |
   *                 | b |
   *                 +---+
   */
  public static byte[][] concatArray2D(byte[][] a, byte[][] b) {
    int k = a.length + b.length;
    byte[][] concatedArray = new byte[k][];
    for (int i = 0; i < k; ++i) {
      if (i < a.length) {
        concatedArray[i] = a[i];
      } else {
        concatedArray[i] = b[i - a.length];
      }
    }
    return concatedArray;
  }

  /**
   * Get first several rows of 2D array
   *
   * @param bb 2D array
   * @param k first k rows
   * @return partial 2D array
   */
  public static byte[][] getPartArray2D(byte[][] bb, int k) {
    if (k == 0 || k > bb.length) {
      return null;
    }
    int pos = 0;

    if (k < 0) {
      pos = bb.length + k;
      if (pos < 0) {
        return null;
      }

      k = -k;
    }

    byte[][] partArray2D = new byte[k][];
    System.arraycopy(bb, pos, partArray2D, 0, k);

    return partArray2D;
  }

  /**
   * Print debug info in format: "thread id. file name. line number. message".
   *
   * @param msg message to be printed
   */
  /* for debug */
  public static void printLineInfo(String msg) {
    StackTraceElement ste = new Throwable().getStackTrace()[1];
    System.out.print(Thread.currentThread().getId() + ".");
    System.out.print("File:" + ste.getFileName() + ".");
    System.out.print("Line: " + ste.getLineNumber() + ".");
    if (msg != null) {
      System.out.print("|" + msg + "| ");
    }
    System.out.println();
  }
}
