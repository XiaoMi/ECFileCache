package com.xiaomi.filecache.ec.utils;

import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import org.apache.commons.lang3.ArrayUtils;

public class DataUtil {

  // 2D array operation

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
