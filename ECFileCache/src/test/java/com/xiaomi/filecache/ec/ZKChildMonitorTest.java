package com.xiaomi.filecache.ec;

import com.xiaomi.filecache.ec.zk.ZKChildMonitor;
import org.junit.Test;

public class ZKChildMonitorTest {
    @Test
    public void testZkChildMonitor() {
        ZKChildMonitor monitor = ZKChildMonitor.getInstance();

        System.out.print(monitor.getRedisCluster());
    }
}
