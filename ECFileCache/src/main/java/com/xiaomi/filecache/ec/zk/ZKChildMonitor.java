package com.xiaomi.filecache.ec.zk;

import com.xiaomi.filecache.ec.Config;
import com.xiaomi.filecache.ec.redis.RedisAccessBase;
import com.xiaomi.filecache.ec.redis.RedisAccessParallel;
import com.xiaomi.filecache.ec.redis.RedisAccessSerial;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ZKChildMonitor implements ZKChildListener {

    private static Logger LOGGER = LoggerFactory.getLogger(ZKChildMonitor.class);
    private static final String SLASH = "/";
    private static final String CLUSTER_ID = "cluster_id";
    private static final String ZK_CLUSTER_PATH_FORMAT = "/xmss/filecache/%s";
    private static final String ZK_POOL_PATH = "pool";
    private static final String CLUSTER_CONF_FILE = "/cluster.properties";

    private ZKClient client;
    private volatile Map<Integer, String> redisCluster;
    private volatile RedisAccessBase redisAccess;

    private boolean isRedisAccessParallel = false;


    private static class MonitorHolder {
        static final ZKChildMonitor INSTANCE = new ZKChildMonitor();
    }

    public static ZKChildMonitor getInstance() {
        return MonitorHolder.INSTANCE;
    }

    private ZKChildMonitor() {

        String zkClusterPath = String.format(ZK_CLUSTER_PATH_FORMAT, getClusterId());
        String zkClusterPoolPath = zkClusterPath + SLASH + ZK_POOL_PATH;

        String servers = "zk1.onebox.srv:2182,zk2.onebox.srv:2182,zk3.onebox.srv:2182,zk4.onebox.srv:2182,zk5.onebox.srv:2182";
        client = new ZKClient(servers);

        initConfig(zkClusterPath);
        initRedisAccess(zkClusterPoolPath);
    }

    private String getClusterId() {

        Properties props = new Properties();
        try {
            props.load(ZKChildMonitor.class.getResourceAsStream(CLUSTER_CONF_FILE));
        } catch (IOException e) {
            LOGGER.error("Read cluster.properties exception", e);
        }

        String clusterIdStr = System.getProperty(CLUSTER_ID);
        if (StringUtils.isNotEmpty(clusterIdStr)) {
            LOGGER.warn("Apply the cluster Id from system setting: [{}]", clusterIdStr);
        } else {
            clusterIdStr = props.getProperty(CLUSTER_ID);
            LOGGER.warn("Apply the cluster Id from cluster.properties: [{}]", clusterIdStr);
        }
        Validate.notEmpty(clusterIdStr);

        return clusterIdStr;
    }

    private void initConfig(String clusterPath){
        Properties props = client.getData(Properties.class, clusterPath);
        Config.init(props);
        isRedisAccessParallel = Config.getInstance().isRedisAccessParallel();
    }

    private void initRedisAccess(String clusterPoolPath) {

        client.createPersistent(clusterPoolPath);

        List<String> childrenNames = client.getChildren(clusterPoolPath);
        onChanged(clusterPoolPath, childrenNames);
        Validate.notNull(redisAccess);

        client.registerChildChanges(clusterPoolPath, this);
    }

    @Override
    public void onChanged(String parentPath, List<String> currentChildren) {
        if (currentChildren == null) {
            LOGGER.error("{} is null", parentPath);
            return;
        } else {
            LOGGER.warn("{} is changed to '{}'", parentPath, currentChildren);
        }

        Map<Integer, String> zkRedisCluster = new HashMap<Integer, String>();
        for (String node : currentChildren) {
            String nodeData = client.getData(String.class, parentPath + SLASH + node);
            zkRedisCluster.put(Integer.parseInt(node), nodeData);
        }

        if (MapUtils.isNotEmpty(zkRedisCluster) && !zkRedisCluster.equals(redisCluster)) {
            redisCluster = zkRedisCluster;
            if (isRedisAccessParallel) {
                redisAccess = new RedisAccessParallel(zkRedisCluster);
            } else {
                redisAccess = new RedisAccessSerial(zkRedisCluster);
            }
        }
    }


    public RedisAccessBase get() {
        return redisAccess;
    }

    public Map<Integer, String> getRedisCluster() {
        return redisCluster;
    }
}
