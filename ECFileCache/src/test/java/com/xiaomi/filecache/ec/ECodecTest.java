package com.xiaomi.filecache.ec;

import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import com.xiaomi.filecache.ec.utils.DataUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class ECodecTest {
    Random random = new Random();
    @Test
    public void testEncodeDecode() throws ECFileCacheException {
        int k = 8;
        int m = 3;
        int length = 8 * 1024;
        int totalLenth = k * m * length;
        byte[] data = new byte[totalLenth];
        random.nextBytes(data);

        ECodec ecodec = ECodec.getInstance();

        // encode
        byte[][] dataAndCoding = ecodec.encode(data);
        byte[][] dataBlock = DataUtil.getPartArray2D(dataAndCoding, k);

        Assert.assertArrayEquals(data, DataUtil.array2DToArray(dataBlock));

        // decode
        int erased1 = 1;
        int erased2 = 9;
        int erased3 = 9;
        int [] erasures = new int[2];
        // TODO  erasures[0] = -1 will get jna error
        erasures[0] = erased1;
        erasures[1] = -1;

        Arrays.fill(dataAndCoding[erased1], (byte) 0);

        byte[] restoredData = ecodec.decode(dataAndCoding, erasures);
        Assert.assertArrayEquals(data, restoredData);

    }
}
