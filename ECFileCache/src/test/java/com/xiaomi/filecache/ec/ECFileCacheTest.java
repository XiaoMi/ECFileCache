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
    ECFileCache client = new ECFileCache();


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

        // get file

        byte[] cachedFile = client.getFile(fileCacheKey);
        IOUtils.closeQuietly(inputStream);

        Assert.assertArrayEquals(buffer, cachedFile);

        // get file stream
        inputStream = client.asInputStream(fileCacheKey);
        cachedFile = IOUtils.toByteArray(inputStream);
        IOUtils.closeQuietly(inputStream);

        Assert.assertArrayEquals(buffer, cachedFile);

        // delete file
        client.deleteFile(fileCacheKey);
        try {
            client.getFile(fileCacheKey);
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.startsWith(e.getMessage(), "get cached data fail count"));
        }
    }

    @Test
    public void testPadding() throws IOException, ECFileCacheException {
        Random random = new Random();
        int size = 289597;
        byte[] buffer = new byte[size];
        random.nextBytes(buffer);

        // create file cache key
        String fileCacheKey = client.createFileCacheKey(size);

        // put file
        int chunkPos = 0;
        byte[] chunk = ArrayUtils.subarray(buffer, chunkPos, size);
        InputStream inputStream = new ByteArrayInputStream(chunk);
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
            Assert.assertTrue(StringUtils.startsWith(e.getMessage(), "get cached data fail count"));
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
            Assert.assertTrue(StringUtils.startsWith(e.getMessage(), "get cached data fail count"));
        }
    }

    @Test
    public void tesPutGetWithErasedData() throws IOException, ECFileCacheException {

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
            Assert.assertTrue(StringUtils.startsWith(e.getMessage(), "get cached data fail count"));
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
        Map<Integer, DecoratedJedisPool> keyedPool = client.getRedisAccess().getKeyedPool();
        keyedPool.put(0, null);
        keyedPool.put(1, null);
        testPutGetDeleteImpl();

        keyedPool.remove(2);
        testPutGetDeleteImpl();

        keyedPool.put(3, null);
        keyedPool.put(4, null);
        try {
            testPutGetDeleteImpl();
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.startsWith(e.getMessage(), "get cached data fail count > CODING_BLOCK_NUM."));
        }
    }
}
