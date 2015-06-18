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

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

public class CodecUtils {

  /**
   * Adjusts the erasures array to satisfy the Jerasure library's requirements.
   */
  public static int[] adjustErasures(int[] erasures) {
    int[] adjustedErasures =  Arrays.copyOf(erasures, erasures.length + 1);
    adjustedErasures[erasures.length] = -1;
    return adjustedErasures;
  }

  /**
   * Copies back the decoded and data and coding to the java specified buffers.
   */
  public static void copyBackDecoded(Pointer[] dataPtrs, Pointer[] codingPtrs,
      int[] erasures, byte[][] data,
      byte[][] coding) {
    int k = data.length;
    for (int i = 0; i < erasures.length && erasures[i] >= 0; ++i) {
      if (erasures[i] < k) {
        // Copy back decoded data
        byte[] array = dataPtrs[erasures[i]].getByteArray(0,
            data[erasures[i]].length);
        System.arraycopy(array, 0, data[erasures[i]], 0, array.length);
      } else {
        // Copy back decoded coding
        byte[] array = codingPtrs[erasures[i] - k].getByteArray(0,
            coding[erasures[i] -k].length);
        System.arraycopy(array, 0, coding[erasures[i] -k], 0, array.length);
      }
    }
  }

  /**
   * Converts a java byte[][] array to JNA Pointer[] array.
   */
  public static Pointer[] toPointerArray(byte[][] array) {
    Pointer[] ptrArray = new Pointer[array.length];
    for (int i = 0; i < array.length; ++i) {
      ptrArray[i] = new Memory(array[i].length);
      ptrArray[i].write(0, array[i], 0, array[i].length);
    }
    return ptrArray;
  }

  /**
   * Converts JNA Pointer[] array to java byte[][] array.
   */
  public static void toByteArray(Pointer[] ptrArray, byte[][] array) {
    for (int i = 0; i < array.length; ++i) {
      byte[] arr = ptrArray[i].getByteArray(0, array[i].length);
      System.arraycopy(arr, 0, array[i], 0, array[i].length);
    }
  }

  /**
   * Prints a byte[][] array as a matrix.
   */
  public static void printMatrix(byte[][] matrix, boolean printMatrix) {
    if (printMatrix) {
      for (int i = 0; i < matrix.length; ++i) {
        for (int j = 0; j < matrix[i].length; ++j) {
          System.out.printf("%02x ", matrix[i][j]);
        }
        System.out.println();
      }
      System.out.println();
    }
  }

  /**
   * Prints a int[] array as a matrix.
   */
  public static void printMatrix(int[] matrix, int row, int col,
      boolean printMatrix) {
    if (printMatrix) {
      for (int i = 0; i < row; ++i) {
        for (int j = 0; j < col; ++j) {
          System.out.printf("%02x ", matrix[i * col + j]);
        }
        System.out.println();
      }
      System.out.println();
    }
  }
}
