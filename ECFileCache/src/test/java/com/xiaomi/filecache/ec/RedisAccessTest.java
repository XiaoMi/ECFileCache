package com.xiaomi.filecache.ec;

import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import com.xiaomi.filecache.ec.redis.RedisAccessBase;
import com.xiaomi.filecache.ec.redis.RedisAccessParallel;
import com.xiaomi.filecache.ec.redis.RedisAccessSerial;
import com.xiaomi.filecache.ec.utils.DataUtil;
import com.xiaomi.filecache.ec.utils.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class RedisAccessTest {
    RedisAccessBase access;
    Random random = new Random();

    private static final int CHUNK_SIZE = 64 * 1024;

    public RedisAccessTest() {
        Config.init(new Properties());
    }

    public Map<Integer, String> getRedisCluster() {
        Map<Integer, String> cluster = new HashMap<Integer, String>();
        cluster.put(0, "localhost:6379");
        cluster.put(1, "localhost:6380"); // down
        cluster.put(2, "localhost:6381");
        cluster.put(3, "localhost:6382");
        cluster.put(4, "localhost:6383"); // down
        cluster.put(5, "localhost:6384");
        cluster.put(6, "localhost:6385");
        cluster.put(7, "localhost:6386"); // down
        cluster.put(8, "localhost:6387");
        cluster.put(9, "localhost:6388");
        cluster.put(10, "localhost:6389"); // down
        cluster.put(11, "localhost:6390");
        cluster.put(12, "localhost:6391");
        return cluster;
    }

    @Test
    public void testPutGetDelete() throws ECFileCacheException {
        access = new RedisAccessSerial(getRedisCluster());
        testPutGetDeleteImpl();
        access = new RedisAccessParallel(getRedisCluster());
        testPutGetDeleteImpl();
    }

    private void testPutGetDeleteImpl() throws ECFileCacheException {
        int n = 11;
        int m = 3;
        int k = 8;

        List<Integer> ids = new ArrayList<Integer>();
        for(int i = 0; i < k; ++i){
            ids.add(i);
        }

        long now = System.currentTimeMillis();
        String key = "test_" + Long.toString(now);
        long field = 0;
        byte[] buffer0 = put(ids, key, field, m);

        field = CHUNK_SIZE * 2;
        byte[] buffer2 = put(ids, key, field, m);

        field = CHUNK_SIZE;
        byte[] buffer1 = put(ids, key, field, m);

        List<Pair<byte[][], int[]>> chunkErasures = access.get(ids, key);
        List<byte[]> list = new ArrayList<byte[]>();
        for (Pair<byte[][], int[]> pair : chunkErasures) {
            list.add(DataUtil.array2DToArray(pair.getFirst()));
        }

        Assert.assertArrayEquals(buffer0, list.get(0));
        Assert.assertArrayEquals(buffer1, list.get(1));
        Assert.assertArrayEquals(buffer2, list.get(2));

        // delete
        access.delete(key, ids);
        try {
            access.get(ids, key);
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.startsWith(e.getMessage(), "get cached data fail count > CODING_BLOCK_NUM"));
        }

        // get not exist key
        try {
            access.get(ids, "not_exist");
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.startsWith(e.getMessage(), "get cached data fail count > CODING_BLOCK_NUM"));
        }
    }

    @Test
    public void testPutGetStreamDelete() throws ECFileCacheException {
        access = new RedisAccessSerial(getRedisCluster());
        testPutGetStreamDeleteImple();
        access = new RedisAccessParallel(getRedisCluster());
        testPutGetStreamDeleteImple();
    }

    private void testPutGetStreamDeleteImple() throws ECFileCacheException {
        int m = 3;
        int k = 8;

        List<Integer> ids = new ArrayList<Integer>();
        for(int i = 0; i < k; ++i){
            ids.add(i);
        }

        long now = System.currentTimeMillis();
        String key = "test_" + Long.toString(now);
        long field = 0;
        byte[] buffer0 = put(ids, key, field, m);

        field = CHUNK_SIZE * 2;
        byte[] buffer2 = put(ids, key, field, m);

        field = CHUNK_SIZE;
        byte[] buffer1 = put(ids, key, field, m);

        List<byte[]> list = new ArrayList<byte[]>();
        Map<Long, Integer> chunkPosAndSize = access.getChunkPosAndSize(ids, key);

        for(Map.Entry<Long,Integer> entry : chunkPosAndSize.entrySet()){
            long chunkPos = entry.getKey();
            int chunkSize = entry.getValue();

            Pair<byte[][], int[]> chunkErasures = access.getChunk(key, chunkPos, chunkSize, ids);
            list.add(DataUtil.array2DToArray(chunkErasures.getFirst()));

        }

        Assert.assertArrayEquals(buffer0, list.get(0));
        Assert.assertArrayEquals(buffer1, list.get(1));
        Assert.assertArrayEquals(buffer2, list.get(2));

        // delete
        access.delete(key, ids);
        try {
            access.get(ids, key);
            Assert.fail("should not reach here");
        } catch (ECFileCacheException e) {
            Assert.assertTrue(StringUtils.startsWith(e.getMessage(), "get cached data fail count > CODING_BLOCK_NUM"));
        }
    }

    @Test
    public void testJedis() {

        String host = "localhost";
        int port = 6380;
        String key = "test_xxx";
        String keyNotExist = "test_not_exist";

        String field = "field_1";
        String value = "value_1";

        String fieldNotExist = "field_not_exist";

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(Config.getInstance().getJedisPoolMax());

        JedisPool jedisPool = new JedisPool(jedisPoolConfig, host, port, Config.getInstance().getJedisSocketTimeoutMs(), Config.getInstance().getRedisPassword());
        Jedis jedis = jedisPool.getResource();

        jedis.hset(key.getBytes(), field.getBytes(), value.getBytes());

        // hgetAll
        Map<byte[], byte[]> result = jedis.hgetAll(key.getBytes());
        Assert.assertTrue(MapUtils.isNotEmpty(result));

        result = jedis.hgetAll(keyNotExist.getBytes());
        Assert.assertTrue(MapUtils.isEmpty(result));

        // hkeys
        Set<byte[]> redisKeys = jedis.hkeys(key.getBytes());
        Assert.assertTrue(CollectionUtils.isNotEmpty(redisKeys));

        redisKeys = jedis.hkeys(keyNotExist.getBytes());
        Assert.assertTrue(CollectionUtils.isEmpty(redisKeys));


        // hget
        byte[] redisValue = jedis.hget(key.getBytes(), field.getBytes());
        Assert.assertTrue(!ArrayUtils.isEmpty(redisValue));

        redisValue = jedis.hget(key.getBytes(), fieldNotExist.getBytes());
        Assert.assertTrue(ArrayUtils.isEmpty(redisValue));

        redisValue = jedis.hget(keyNotExist.getBytes(), field.getBytes());
        Assert.assertTrue(ArrayUtils.isEmpty(redisValue));

    }

    private byte[] put(List<Integer> ids, String key, long field, int m) throws ECFileCacheException {
        byte[] buffer = new byte[CHUNK_SIZE];
        random.nextBytes(buffer);
        byte[][] data = DataUtil.arrayToArray2D(buffer, 8);

        access.put(ids, key, field, data);
        return buffer;
    }
}
