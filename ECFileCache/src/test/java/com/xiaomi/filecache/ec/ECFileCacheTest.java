package com.xiaomi.filecache.ec;

import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import com.xiaomi.filecache.ec.redis.DecoratedJedisPool;
import com.xiaomi.filecache.ec.redis.RedisAccessBase;
import com.xiaomi.filecache.ec.utils.SerializationHelper;
import com.xiaomi.filecache.thrift.FileCacheKey;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ECFileCacheTest {
    short clusterId = 20005;
    short partitionId = 0;
    ECFileCache client = new ECFileCache(clusterId, partitionId);

    final boolean eraseRedis = false; // set to true for test cache data lost

    public ECFileCacheTest() {
        if (eraseRedis) {
            Map<Integer, DecoratedJedisPool> keyedPool = client.getRedisAccess().getKeyedPool();
            keyedPool.put(0, null);
            keyedPool.put(1, null);
        }
    }

    @Test
    public void testPutGetDelete() throws IOException, ECFileCacheException {
        testPutGetDeleteImpl();
    }

    private void testPutGetDeleteImpl() throws IOException, ECFileCacheException {

        Random random = new Random();
        int size = 4 * 64 * 1024;
        byte[] buffer = new byte[size];
        random.nextBytes(buffer);

        // create file cache key
        String fileCacheKey = client.createFileCacheKey(size);

        // put file
        int chunkPos = 0;
        int endPos = 64 * 1024;
        putChunk(fileCacheKey, buffer, chunkPos, endPos);

        chunkPos = endPos;
        endPos += 2 * 64 * 1024;
        putChunk(fileCacheKey, buffer, chunkPos, endPos);

        chunkPos = endPos;
        endPos += 64 * 1024;
        putChunk(fileCacheKey, buffer, chunkPos, endPos);

        // get file
        byte[] cachedFile = client.getFile(fileCacheKey);

        Assert.assertArrayEquals(buffer, cachedFile);

        // get file stream
        InputStream inputStream = client.asInputStream(fileCacheKey);
        cachedFile = IOUtils.toByteArray(inputStream);
        IOUtils.closeQuietly(inputStream);

        Assert.assertArrayEquals(buffer, cachedFile);

        // delete file
        client.deleteFile(fileCacheKey);
        try {
            client.getFile(fileCacheKey);
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "fail count > CODING_BLOCK_NUM."));
        }
    }

    @Test
    public void testPutGetDeleteWithEndChunk() throws IOException, ECFileCacheException {
        int[] sizes = {522, 4 * 64 * 1024 + 510, 8 * 1024 - 1, 8 * 1024, 8 * 1024 + 1, 64 * 1024 - 1, 64 * 1024};
        for (int size : sizes) {
            System.out.println("test size:" + size);
            testPutGetDeleteWithEndChunkImpl(size);
        }
    }

    private void testPutGetDeleteWithEndChunkImpl(int size) throws IOException, ECFileCacheException {

        int unit = 64 * 1024;
        byte[] buffer = new byte[size];
        Random random = new Random();
        random.nextBytes(buffer);

        // create file cache key
        String fileCacheKey = client.createFileCacheKey(size);

        // put file
        int chunkPos;
        int endPos = 0;
        while (endPos + unit < size) {
            chunkPos = endPos;
            endPos += unit;
            putChunk(fileCacheKey, buffer, chunkPos, endPos);
        }

        // get file stream with end chunk
        chunkPos = endPos;
        byte[] endChunk = ArrayUtils.subarray(buffer, chunkPos, size);

        InputStream inputStream;
        if (size > unit) {
            inputStream = client.asInputStream(fileCacheKey, new ByteArrayInputStream(endChunk));
        } else {
            try {
                inputStream = client.asInputStream(fileCacheKey, new ByteArrayInputStream(endChunk));
                Assert.fail("should not reach here");
            } catch (ECFileCacheException e) {
                Assert.assertTrue(StringUtils.contains(e.getMessage(), "fail count > CODING_BLOCK_NUM."));
                return;
            }
        }

        byte[] cachedFile = IOUtils.toByteArray(inputStream);
        IOUtils.closeQuietly(inputStream);

        Assert.assertArrayEquals(buffer, cachedFile);

        // delete file
        client.deleteFile(fileCacheKey);
        try {
            client.getFile(fileCacheKey);
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "fail count > CODING_BLOCK_NUM."));
        }
    }

    private void putChunk(String fileCacheKey, byte[] buffer, int chunkPos, int endPos) throws ECFileCacheException {
        byte[] chunk = ArrayUtils.subarray(buffer, chunkPos, endPos);
        InputStream inputStream = new ByteArrayInputStream(chunk);
        client.putFile(fileCacheKey, chunkPos, inputStream, 0);
        IOUtils.closeQuietly(inputStream);
    }

    @Test
    public void testPadding() throws IOException, ECFileCacheException {

        int[] sizeArray = {289597, 8 * 1024, 8 * 1024 - 1, 8 * 1024 + 1, 5 * 1024 - 1, 11 * 1024 + 1, 64 * 1024 - 1};
        for (int size : sizeArray) {
            testPaddingImpl(size);
        }
    }

    private void testPaddingImpl(int size) throws IOException, ECFileCacheException {
        Random random = new Random();
        byte[] buffer = new byte[size];
        random.nextBytes(buffer);

        // create file cache key
        String fileCacheKey = client.createFileCacheKey(size);

        // put file
        int chunkPos = 0;
        InputStream inputStream = new ByteArrayInputStream(buffer);
        client.putFile(fileCacheKey, chunkPos, inputStream, 0);

        // get file
        byte[] cachedFile = client.getFile(fileCacheKey);
        Assert.assertArrayEquals(buffer, cachedFile);

        InputStream getInputStream =  client.asInputStream(fileCacheKey);
        Assert.assertArrayEquals(buffer, IOUtils.toByteArray(getInputStream));

        // delete file
        client.deleteFile(fileCacheKey);
        try {
            client.getFile(fileCacheKey);
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "fail count > CODING_BLOCK_NUM"));
        }
    }

    @Test
    public void testMultiChunkWithPadding() throws IOException, ECFileCacheException {
        Random random = new Random();

        int fileSize = 0;
        int size = 64 * 1024;
        fileSize += size;
        byte[] buffer0 = new byte[size];
        random.nextBytes(buffer0);

        size = 3756;
        fileSize += size;
        byte[] buffer1 = new byte[size];
        random.nextBytes(buffer1);

        byte[] buffer = ArrayUtils.addAll(buffer0, buffer1);

        // create file cache key
        String fileCacheKey = client.createFileCacheKey(fileSize);

        // put file
        int chunkPos = 0;
        InputStream inputStream = new ByteArrayInputStream(buffer0);
        client.putFile(fileCacheKey, chunkPos, inputStream, 0);

        chunkPos += buffer0.length;
        inputStream = new ByteArrayInputStream(buffer1);
        client.putFile(fileCacheKey, chunkPos, inputStream, 0);

        // get file
        byte[] cachedFile = client.getFile(fileCacheKey);
        Assert.assertArrayEquals(buffer, cachedFile);

        InputStream getInputStream =  client.asInputStream(fileCacheKey);
        Assert.assertArrayEquals(buffer, IOUtils.toByteArray(getInputStream));

        // delete file
        client.deleteFile(fileCacheKey);
        try {
            client.getFile(fileCacheKey);
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "fail count > CODING_BLOCK_NUM."));
        }
    }

    @Test
    public void tesPutGetWithErasedData() throws IOException, ECFileCacheException {
        if (eraseRedis) {
            System.out.println("some redis data unreachable, do not run this case: " +
                    ((new Exception()).getStackTrace())[0].getMethodName());
            return;
        }

        Random random = new Random();
        int size = 4 * 64 * 1024;
        byte[] buffer = new byte[size];
        random.nextBytes(buffer);

        // create file cache key
        String fileCacheKey = client.createFileCacheKey(size);

        // put file
        int chunkPos = 0;
        int endPos = 64 * 1024;
        byte[] chunk = ArrayUtils.subarray(buffer, chunkPos, endPos);

        InputStream inputStream = new ByteArrayInputStream(chunk);
        client.putFile(fileCacheKey, chunkPos, inputStream, 0);

        chunkPos = endPos;
        endPos += 2 * 64 * 1024;
        chunk = ArrayUtils.subarray(buffer, chunkPos, endPos);
        inputStream = new ByteArrayInputStream(chunk);
        client.putFile(fileCacheKey, chunkPos, inputStream, 0);

        chunkPos = endPos;
        endPos += 64 * 1024;
        chunk = ArrayUtils.subarray(buffer, chunkPos, endPos);
        inputStream = new ByteArrayInputStream(chunk);
        client.putFile(fileCacheKey, chunkPos, inputStream, 0);

        // delete part of data in redis
        List<Integer> ids = new ArrayList<Integer>();
        FileCacheKey fileCacheKeyObj = SerializationHelper.toThriftObject(FileCacheKey.class, Base64.decodeBase64(fileCacheKey));
        int offset = fileCacheKeyObj.getDeviceOffset();
        for (int i = 0; i < 3; ++i) {
            ids.add((offset + i) % fileCacheKeyObj.getDeviceClusterSize());
        }
        RedisAccessBase redisAccess = client.getRedisAccess();
        redisAccess.delete(fileCacheKeyObj.getUuid(), ids);

        // get file
        byte[] cachedFile = client.getFile(fileCacheKey);

        Assert.assertArrayEquals(buffer, cachedFile);

        // delete more part of data
        ids.add((offset + 3) % fileCacheKeyObj.getDeviceClusterSize());
        redisAccess.delete(fileCacheKeyObj.getUuid(), ids);

        try {
            client.getFile(fileCacheKey);
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "fail count > CODING_BLOCK_NUM."));
        }
    }

    @Test
    public void tesPutGetMultiThread() throws IOException, InterruptedException {

        int threadNum = 12;
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum);
        pool.prestartAllCoreThreads();

        List<Future<?>> result = new ArrayList<Future<?>>();

        for (int i = 0; i < threadNum; ++i) {
            Future<?> future = pool.submit(new Callable<Object>() {
                @Override
                public Object call() {
                    try {
                        testPutGetDelete();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ECFileCacheException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });
            result.add(future);
        }

        pool.shutdown();

        for (Future<?> future : result) {
            try {
                future.get(5, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
        pool.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    public void testInvalidRedis() throws IOException, ECFileCacheException {
        if (eraseRedis) {
            System.out.println("some redis data unreachable, do not run this case: " +
                    ((new Exception()).getStackTrace())[0].getMethodName());
            return;
        }

        Map<Integer, DecoratedJedisPool> keyedPool = client.getRedisAccess().getKeyedPool();
        Map<Integer, DecoratedJedisPool> storedKeyedPool = new HashMap<Integer, DecoratedJedisPool>(keyedPool);

        // cache data ok when offline redis instance little than 3
        keyedPool.put(0, null);
        keyedPool.put(1, null);
        testPutGetDeleteImpl();

        // cache data fail when offline redis instance more than 3
        keyedPool.put(0, null);
        keyedPool.put(1, null);
        keyedPool.put(2, null);
        keyedPool.put(3, null);
        try {
            testPutGetDeleteImpl();
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "fail count > CODING_BLOCK_NUM."));
        }

        // restore redis 3
        keyedPool.put(3, storedKeyedPool.get(3));

        // allocate cacheKey if remove little than three redis
        keyedPool.remove(0);
        keyedPool.remove(1);
        keyedPool.remove(2);
        testPutGetDeleteImpl();

        // can not allocate cacheKey if remove more than three redis
        keyedPool.remove(3);
        try {
            testPutGetDeleteImpl();
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.startsWith(e.getMessage(), "can not allocate offset id for cacheKey"));
        }

        // restore client pool
        for (int i = 0; i < 5; ++i) {
            keyedPool.put(i, storedKeyedPool.get(i));
        }

        for (int i = 0; i < 5; ++i) {
            Assert.assertNotNull(keyedPool.get(i));
        }
    }

    @Test
    public void testPutAndGetInfo() throws ECFileCacheException {
        if (eraseRedis) {
            System.out.println("some redis data unreachable, do not run this case: " +
                    ((new Exception()).getStackTrace())[0].getMethodName());
            return;
        }

        Random random = new Random();
        int size = 4 * 64 * 1024;
        byte[] buffer = new byte[size];
        random.nextBytes(buffer);

        // create file cache key
        String fileCacheKey = client.createFileCacheKey(size);

        // test put and get success
        client.putExtraInfo(fileCacheKey, buffer);
        byte[] result = client.getExtraInfo(fileCacheKey);

        Assert.assertNotNull(result);
        Assert.assertTrue(ArrayUtils.isEquals(buffer, result));

        // get object key with invalid cache key
        fileCacheKey = client.createFileCacheKey(size);
        result = client.getExtraInfo(fileCacheKey);
        Assert.assertNull(result);
    }

    @Test
    public void testReUploadChunkWithDifferentSize() throws ECFileCacheException, IOException {
        Random random = new Random();
        int size = 6 * 64 * 1024;
        byte[] buffer = new byte[size];
        random.nextBytes(buffer);

        // create file cache key
        String fileCacheKey = client.createFileCacheKey(size);

        // put file
        int chunkPos = 0;
        int endPos = 64 * 1024;
        putChunk(fileCacheKey, buffer, chunkPos, endPos);

        // upload a dummy chunk
        chunkPos = endPos;
        endPos += 2 * 64 * 1024;
        putChunk(fileCacheKey, buffer, chunkPos, endPos);

        // re upload chunk of chunkPos, with different chunkSize
        endPos = chunkPos + 64 * 1024;
        putChunk(fileCacheKey, buffer, chunkPos, endPos);

        chunkPos = endPos;
        endPos += 64 * 1024;
        putChunk(fileCacheKey, buffer, chunkPos, endPos);
        // //// end re upload

        // upload a dummy chunk
        chunkPos = endPos;
        endPos += 64 * 1024;
        putChunk(fileCacheKey, buffer, chunkPos, endPos);

        // re upload chunk
        endPos = chunkPos + 2 * 64 * 1024;
        putChunk(fileCacheKey, buffer, chunkPos, endPos);
        ////  end re upload

        chunkPos = endPos;
        endPos += 64 * 1024;
        putChunk(fileCacheKey, buffer, chunkPos, endPos);

        // get file
        byte[] cachedFile = client.getFile(fileCacheKey);
        Assert.assertArrayEquals(buffer, cachedFile);

        // get file stream
        InputStream inputStream = client.asInputStream(fileCacheKey);
        cachedFile = IOUtils.toByteArray(inputStream);
        IOUtils.closeQuietly(inputStream);

        Assert.assertArrayEquals(buffer, cachedFile);

        // delete file
        client.deleteFile(fileCacheKey);
        try {
            client.getFile(fileCacheKey);
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "fail count > CODING_BLOCK_NUM."));
        }
    }
}
