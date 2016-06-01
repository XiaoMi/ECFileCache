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
    private static final String ZK_CLUSTER_PATH_FORMAT = "/xmss/clusters/%d/filecache";
    private static final String ZK_PARTITION_PATH_FORMAT = ZK_CLUSTER_PATH_FORMAT + "/partition_%d/pool";
    private static final String ZK_SERVERS = "zk_servers";
    private static final String CLUSTER_CONF_FILE = "/cluster.properties";

    private String zkServers;

    private ZKClient client;
    private volatile Map<Integer, String> redisCluster;
    private volatile RedisAccessBase redisAccess;

    private boolean isRedisAccessParallel = false;
    private short clusterId;
    private short partitionId;

    private static volatile ZKChildMonitor instance = null;

    public static ZKChildMonitor getInstance(short clusterId, short partitionId) {
        if (instance == null) {
            synchronized (ZKChildMonitor.class) {
                if (instance == null) {
                    instance = new ZKChildMonitor(clusterId, partitionId);
                    LOGGER.info("init ZkChildMonitor with clusterId[{}]", clusterId);
                }
            }
        } else {
            Validate.isTrue(clusterId == instance.clusterId && partitionId == instance.partitionId,
                    String.format("ZkChildMonitor initialized with id[%d], reject id[%d]", instance.clusterId, clusterId));
        }
        return instance;
    }


    private ZKChildMonitor(short clusterId, short partitionId) {

        this.clusterId = clusterId;
        this.partitionId = partitionId;

        loadZkInfos();
        client = new ZKClient(zkServers);

        String zkClusterPath = String.format(ZK_CLUSTER_PATH_FORMAT, clusterId);
        String zkPartitionPath = String.format(ZK_PARTITION_PATH_FORMAT, clusterId, partitionId);

        initConfig(zkClusterPath);
        initRedisAccess(zkPartitionPath);
    }

    private void loadZkInfos() {

        Properties props = new Properties();
        try {
            props.load(ZKChildMonitor.class.getResourceAsStream(CLUSTER_CONF_FILE));
        } catch (IOException e) {
            LOGGER.error("Read cluster.properties exception", e);
        }

        String zkServersStr = System.getProperty(ZK_SERVERS);
        if (StringUtils.isNotEmpty(zkServersStr)) {
            LOGGER.warn("Apply the zk servers from system setting: [{}]", zkServersStr);
        } else {
            zkServersStr = props.getProperty(ZK_SERVERS);
            LOGGER.warn("Apply the zk servers from cluster.properties: [{}]", zkServersStr);
        }
        Validate.notEmpty(zkServersStr);

        zkServers = zkServersStr;
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
