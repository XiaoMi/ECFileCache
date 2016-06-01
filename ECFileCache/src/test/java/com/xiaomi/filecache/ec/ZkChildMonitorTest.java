package com.xiaomi.filecache.ec;

import com.xiaomi.filecache.ec.zk.ZKChildMonitor;
import org.junit.Test;

public class ZkChildMonitorTest {
    @Test
    public void testZkChildMonitor() {
        short clusterId = 20002;
        short partitionId = 0;
        ZKChildMonitor monitor = ZKChildMonitor.getInstance(clusterId, partitionId);

        System.out.print(monitor.getRedisCluster());
    }
}
