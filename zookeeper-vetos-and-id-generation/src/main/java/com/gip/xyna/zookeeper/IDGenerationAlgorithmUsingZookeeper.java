package com.gip.xyna.zookeeper;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.CachedAtomicLong;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryForever;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IDGenerationAlgorithmUsingZookeeper /* implements IdGenerationAlgorithm */ {

    private static final String ZK_CONNECTION_STRING = "zookeeper:2181";
    private static final String XYNA_FACTORY_ZK_NAMESPACE = "com.gip.xyna.factory.distributed";
    private static final String COUNTER_PATH = "/idgeneration";
    private static final String LOCK_PATH = "/idgeneration.locks";

    private static int DEFAULT_INCREMENT = 50_000;
    private static String REALM_DEFAULT = "default";

    Logger log = LogManager.getLogger(this.getClass());
    CuratorFramework zkc;

    ConcurrentHashMap<String, CachedAtomicLong> counters = new ConcurrentHashMap<>();

    /**
     * IdGenerationAlgorithm intialisieren
     */
    void init(String connecString) /* throws PersistenceLayerException */ {
        zkc = CuratorFrameworkFactory.newClient(connecString,
                new ExponentialBackoffRetry(1000, 3));

        try {
            zkc.start();
            zkc.blockUntilConnected();
        } catch (Exception e) {
            log.error("Could not start Zookeeper client for ID generation.");
            throw new RuntimeException("Could not start Zookeeper client for ID generation.", e);
        }

    }

    void init() {
        init(ZK_CONNECTION_STRING);
    }

    void init(CuratorFramework client) {

        if (zkc != null)
            try {
                zkc.close();
            } catch (UnsupportedOperationException e) {

            }

        zkc = client;
    }

    /**
     * IdGenerationAlgorithm beenden
     */
    void shutdown() /* throws PersistenceLayerException */ {
        if (zkc != null)
            try {
                zkc.close();
            } catch (UnsupportedOperationException e) {
            }
    }

    /**
     * @param realm
     * @return neue eindeutige Id
     */
    long getUniqueId(String realm) {

        CachedAtomicLong realmCounter = counters.get(realm);

        if (realmCounter == null) {

            // https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html#ch_zkDataModel
            // https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/main/java/org/apache/zookeeper/common/PathUtils.java#L43
            // and replace every /
            String sanitizedRealm = realm.replaceAll("[\u0000-\u001f\u007f-\u009F\ud800-\uf8ff\ufff0-\uffff/]", "_");

            if (log.isWarnEnabled() && !sanitizedRealm.equals(realm))
                log.warn("using sanitized Realm '" + sanitizedRealm + "' for realm '" + realm + "'");
            // System.out.println("using sanitized Realm '" + sanitizedRealm + "' for realm
            // '" + realm + "'");

            DistributedAtomicLong counter = new DistributedAtomicLong(
                    zkc.usingNamespace(XYNA_FACTORY_ZK_NAMESPACE),
                    COUNTER_PATH + "/" + sanitizedRealm,
                    new ExponentialBackoffRetry(100, 5),
                    PromotedToLock.builder()
                            .lockPath(LOCK_PATH + "/" + sanitizedRealm)
                            .retryPolicy(new RetryForever(500))
                            .build());

            try {
                if (/* IDGenerator. */REALM_DEFAULT.equals(realm)) {
                    counter.initialize(/* IDGenerator. */getID_OFFSET()); // only sets initialvalue on first creation in
                                                                          // zookeeper
                }
            } catch (Exception e) { // should not happen as we retry forever
                log.error("Could not initalize Zookeeper counter for ID generation in realm " + realm);
                throw new RuntimeException("Could not initalize Zookeeper counter for ID generation in realm " + realm,
                        e);
            }

            CachedAtomicLong newCounter = new CachedAtomicLong(
                    counter,
                    DEFAULT_INCREMENT);

            realmCounter = counters.putIfAbsent(realm, newCounter);
            if (realmCounter == null)
                realmCounter = newCounter;
        }

        try {
            synchronized (realmCounter) {
                var next = realmCounter.next();
                while (!next.succeeded()) {
                    next = realmCounter.next();
                }

                return next.postValue();
            }
        } catch (Exception e) { // should not happen as we retry forever
            log.error("Could not get Zookeeper counter for ID generation in realm " + realm);
            throw new RuntimeException("Could not get Zookeeper counter for ID generation in realm " + realm, e);
        }

    }

    private Long getID_OFFSET() {
        return 10_000L;
    }

    long getIdLastUsedByOtherNode(String realm) {
        throw new RuntimeException("Unsupported function.");

    }

    void storeLastUsed(String realm) {
        // ntbd
    };
}
