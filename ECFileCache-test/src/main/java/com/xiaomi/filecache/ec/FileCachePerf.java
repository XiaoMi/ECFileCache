package com.xiaomi.filecache.ec;

import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import com.xiaomi.filecache.ec.utils.DataUtil;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.reporting.ConsoleReporter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class FileCachePerf {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileCachePerf.class);


    private Timer uploadTimer = null;
    private Timer downloadTimer = null;
    private Counter failCounter = null;

    private boolean stop = false;

    private ECFileCache ECFileCache;

    private long reportInterval = 5;
    private long reportCount = 30;
    private int fileSize = 64 * 1024;
    int chunkNum = fileSize / 64 / 1024;

    private int threadNum = 12;
    private int threadUploadFileNum = -1;

    private  boolean getStream;

    FileCachePerf(AppConfig config) {
        ECFileCache = new ECFileCache(config.clusterId, (short) 0);

        this.threadNum = config.threadNum;
        this.getStream = config.getStream;
        this.fileSize = config.fileSize;
        this.chunkNum = fileSize / 64 / 1024;
    }

    public void run() {

        final long runDuration = reportInterval * reportCount * 1000;
        ExecutorService moniter = Executors.newSingleThreadExecutor();
        moniter.submit(new Runnable() {
            @Override
            public void run() {

                System.out.println("init metrics");
                uploadTimer = Metrics.newTimer(FileCachePerf.class, "upload");
                downloadTimer = Metrics.newTimer(FileCachePerf.class, "download");
                failCounter = Metrics.newCounter(FileCachePerf.class, "failCount");
                Metrics.newGauge(FileCachePerf.class, "uploadThroughput", new Gauge<Double>() {
                    @Override
                    public Double value() {
                        return fileSize * 8 * uploadTimer.meanRate() / 1024 / 1024;
                    }
                });
                Metrics.newGauge(FileCachePerf.class, "downloadThroughput", new Gauge<Double>() {
                    @Override
                    public Double value() {
                        return fileSize * 8 * downloadTimer.meanRate() / 1024 / 1024;
                    }
                });
                ConsoleReporter consoleReporter = new ConsoleReporter(Metrics.defaultRegistry(),
                        System.out,
                        MetricPredicate.ALL
                );
                consoleReporter.start(reportInterval, TimeUnit.SECONDS);
                System.out.println("init metrics finish");

                // run duration
                try {
                    Thread.sleep(runDuration);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    consoleReporter.shutdown();
                    Metrics.defaultRegistry().removeMetric(FileCachePerf.class, "upload");
                    Metrics.defaultRegistry().removeMetric(FileCachePerf.class, "download");
                    Metrics.defaultRegistry().removeMetric(FileCachePerf.class, "uploadThroughput");
                    Metrics.defaultRegistry().removeMetric(FileCachePerf.class, "downloadThroughput");
                    Metrics.defaultRegistry().removeMetric(FileCachePerf.class, "failCount");
                    stop = true;
                }
            }
        });

        System.out.println("start upload");
        ThreadPoolExecutor uploadPool = multiThreadUpload(threadNum, threadUploadFileNum);
        System.out.println("start upload finish");

        moniter.shutdown();
        try {
            if (uploadPool != null) {
                uploadPool.awaitTermination(runDuration, TimeUnit.MILLISECONDS);
            }

            moniter.awaitTermination(runDuration, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("wait for ending failed", e);
        }
    }

    private ThreadPoolExecutor multiThreadUpload(int threadNum, final int threadFileNum) {

        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum);
        pool.prestartAllCoreThreads();

        for (int i = 0; i < threadNum; ++i) {
            final int threadId = i;
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    uploadPerform(threadId, threadFileNum);
                }
            });
        }
        pool.shutdown();
        return pool;
    }

    private void uploadPerform(int threadId, int threadFileNum) {
        byte[] buffer = new byte[fileSize];
        Random random = new Random();
        random.nextBytes(buffer);

        for (int fileCount = threadFileNum; fileCount != 0 && !stop; --fileCount) {
            byte[][] chunks;
            try {
                chunks = DataUtil.arrayToArray2D(buffer, chunkNum);
            } catch (ECFileCacheException e) {
                throw new RuntimeException("split array fail", e);
            }

            // upload file
            InputStream inputStream = null;
            try {
                String key = ECFileCache.createFileCacheKey(fileSize);

                // start uploadTiming
                TimerContext uploadTiming = null;
                if (uploadTimer != null) {
                    uploadTiming = uploadTimer.time();
                }

                int chunkPos = 0;
                for (byte[] chunk : chunks) {
                    ECFileCache.putFile(key, chunkPos, new ByteArrayInputStream(chunk), 1);
                    chunkPos += chunk.length;
                }

                // stop uploadTiming
                if (uploadTiming != null) {
                    uploadTiming.stop();
                }

                TimerContext downloadTiming = null;
                if (downloadTimer != null) {
                    downloadTiming = downloadTimer.time();
                }
                byte[] buf;
                if (getStream) {
                    inputStream = ECFileCache.asInputStream(key);
                    buf = IOUtils.toByteArray(inputStream);
                } else {
                    buf = ECFileCache.getFile(key);
                }

                if (downloadTiming != null) {
                    downloadTiming.stop();
                }

                Validate.isTrue(ArrayUtils.isEquals(buffer, buf));

            } catch (Exception e) {
                LOGGER.error("PERF TEST:perform test fail in thread id {} ", threadId, e);

                if (failCounter != null) {
                    failCounter.inc();
                }
            } finally {
                IOUtils.closeQuietly(inputStream);
            }
        }
    }
}
