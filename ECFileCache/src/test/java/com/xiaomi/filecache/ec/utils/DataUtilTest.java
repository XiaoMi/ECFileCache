package com.xiaomi.filecache.ec.utils;

import org.junit.Assert;
import org.junit.Test;

public class DataUtilTest {
  @Test
  public void testConcatArray2D() {
    int k = 8, m = 3;
    int n = k + m;
    int len = 8;

    byte[][] expected = new byte[n][len];
    byte[][] a = new byte[k][len];
    byte[][] b = new byte[m][len];

    for (int i = 0; i < n; ++i) {
      for (int j = 0; j < len; ++j) {
        if (i < k) {
          a[i][j] = (byte) (i + j);
        } else {
          b[i - k][j] = (byte) (i + j);
        }

        expected[i][j] = (byte) (i + j);
      }
    }

    byte[][] ab = DataUtil.concatArray2D(a, b);
    assertArray2DEquals(expected, ab);
  }

  @Test
  public void testGetPartArray2D(){
    int k = 8, m = 3;
    int n = k + m;
    int len = 8;

    byte[][] bb = new byte[n][len];
    byte[][] a = new byte[k][len];
    byte[][] b = new byte[m][len];

    for (int i = 0; i < n; ++i) {
      for (int j = 0; j < len; ++j) {
        if (i < k) {
          a[i][j] = (byte) (i + j);
        } else {
          b[i - k][j] = (byte) (i + j);
        }

        bb[i][j] = (byte) (i + j);
      }
    }

    byte[][] headK = DataUtil.getPartArray2D(bb, k);
    assertArray2DEquals(a, headK);

    byte[][] tailM = DataUtil.getPartArray2D(bb, -m);
    assertArray2DEquals(b, tailM);
  }

  private void assertArray2DEquals(byte[][] expected, byte[][] actual) {
    Assert.assertEquals(expected.length, actual.length);

    for (int i = 0; i < expected.length; ++i) {
      Assert.assertArrayEquals(expected[i], actual[i]);
    }
  }

  @Test
  public void testInt2Byte() {
    int i = 129;
    byte b = int2byte(i);
    int n = byte2int(b);
    System.out.println("i:" + i);
    System.out.println("b:" + b);
    System.out.println("n:" + n);

  }

  private byte int2byte(int i) {
    return (byte) i;
  }

  private int byte2int(byte b) {
    return (b & 0xFF);
  }

  @Test
  public void testVarArgs() {
    int[] n = new int[3];
    n[0] = 10;
    n[1] = 11;
    n[2] = 12;

    printN("array:", n);

    printN("num:", 20, 21, 22);
  }

  private void printN(String msg, int... n) {
    System.out.println(msg);
    for (int i : n) {
      System.out.print(" " + i + " ");
    }
    System.out.println();
  }
}
