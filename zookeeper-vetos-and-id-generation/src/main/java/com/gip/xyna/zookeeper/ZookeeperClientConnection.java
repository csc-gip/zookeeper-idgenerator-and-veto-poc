package com.gip.xyna.zookeeper;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.common.PathUtils;

public class ZookeeperClientConnection {
    private static String ZK_CONNECTION_STRING = "zookeeper:2181";
    private static String XYNA_FACTORY_ZK_NAMESPACE = "com.gip.xyna.factory.distributed";

    private final static AtomicBoolean isConnected = new AtomicBoolean(false);
    private final static AtomicBoolean isInitialized = new AtomicBoolean(false);

    private static Logger log = LogManager.getLogger(ZookeeperClientConnection.class);
    private static CuratorFramework zk_client;

    private static final class ZookeeperClientInstance {
        static final ZookeeperClientConnection INSTANCE = new ZookeeperClientConnection();
    }

    private ZookeeperClientConnection() {
    }

    private static synchronized void init() {
        if (isInitialized.get())
            return;

        zk_client = CuratorFrameworkFactory.builder()
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .sessionTimeoutMs(60000)
                .connectionTimeoutMs(5000)
                .connectString(ZK_CONNECTION_STRING).build();

        zk_client.getConnectionStateListenable().addListener(new ConnectionStateListener() {

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                isConnected.set(newState.isConnected());
                if (log.isDebugEnabled())
                    log.debug("New connection state for Zookeeper client@"
                            + zk_client.getZookeeperClient().getCurrentConnectionString() + ": " + newState.toString());
            }

        });

        if (!isConnected.get())
            try {
                if (zk_client.getState().equals(CuratorFrameworkState.LATENT))
                    zk_client.start();

                zk_client.blockUntilConnected();
            } catch (Exception e) {
                log.error("Could not start Zookeeper client.");
                throw new RuntimeException("Could not start Zookeeper client.", e);
            }

        isInitialized.set(true);
    }

    public static void setConnectString(String connectString) throws IllegalStateException {
        if (isInitialized.get())
            throw new IllegalStateException("Can not change connect string after initialization.");

        ZK_CONNECTION_STRING = connectString;
    }

    public String getConnectString() {
        return zk_client.getZookeeperClient().getCurrentConnectionString();
    }

    public static void setXynaFactoryZkNamespace(String namespace) {
        XYNA_FACTORY_ZK_NAMESPACE = namespace;
    }

    public static String getXynaFactoryZkNamespace() {
        return XYNA_FACTORY_ZK_NAMESPACE;
    }

    public static ZookeeperClientConnection getInstance() {
        if (isInitialized.get())
            return ZookeeperClientInstance.INSTANCE;

        init();
        return ZookeeperClientInstance.INSTANCE;
    }

    public boolean isConnected() {
        return isConnected.get();
    }

    public void close() {
        zk_client.close();
    }

    public CuratorFramework getCuratorFramework() {
        return zk_client.usingNamespace(XYNA_FACTORY_ZK_NAMESPACE);
    }

    public CuratorFramework getCuratorFrameworkForNamespace(String namespace)
            throws IllegalArgumentException {

        PathUtils.validatePath(namespace);

        return zk_client.usingNamespace(XYNA_FACTORY_ZK_NAMESPACE + namespace);
    }

}
