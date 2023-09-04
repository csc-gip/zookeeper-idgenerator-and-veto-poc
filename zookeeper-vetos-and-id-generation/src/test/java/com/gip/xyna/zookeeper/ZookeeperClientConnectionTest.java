package com.gip.xyna.zookeeper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.FailedServerStartException;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

public class ZookeeperClientConnectionTest extends BaseClassForTests {

    private ZookeeperClientConnection zookeeperClient;
    private static TestingCluster cluster;

    @BeforeAll
    static void init() {
        cluster = new TestingCluster(3);
        try {
            cluster.start();
        } catch (FailedServerStartException e) {
            try {
                cluster.close();
            } catch (Exception ex) {
            }
            cluster = new TestingCluster(3);
            try {
                cluster.start();
            } catch (Exception e1) {
            }
        } catch (Exception e) {
        }

        assertNotNull(cluster);
        final String connectString = cluster.getConnectString();

        ZookeeperClientConnection.setConnectString(connectString);
    }

    @AfterAll
    static void shutdown() {
        try {
            ZookeeperClientConnection.getInstance().close();
            cluster.stop();
        } catch (IOException e) {
        }
    }

    @BeforeEach
    void setUp() {
        zookeeperClient = ZookeeperClientConnection.getInstance();
    }

    @Test
    void testGetInstance() {
        assertNotNull(zookeeperClient);
        assertSame(zookeeperClient, ZookeeperClientConnection.getInstance());
    }

    @Test
    void testSetConnectString() {
        assertThrows(IllegalStateException.class, () -> {
            ZookeeperClientConnection.setConnectString("new_connection_string");
        });
    }

    @Test
    @Order(value = 10)
    void testGetXynaFactoryZkNamespace() {
        assertEquals("com.gip.xyna.factory.distributed", ZookeeperClientConnection.getXynaFactoryZkNamespace());
    }

    @Test
    @Order(value = 90)
    void testSetXynaFactoryZkNamespace() {
        ZookeeperClientConnection.setXynaFactoryZkNamespace("new_namespace");
        assertEquals("new_namespace", ZookeeperClientConnection.getXynaFactoryZkNamespace());
    }

    @Test
    void testIsConnected() {
        assertTrue(zookeeperClient.isConnected());
    }

    @Test
    void testGetCuratorFramework() {
        CuratorFramework curatorFramework = zookeeperClient.getCuratorFramework();
        assertNotNull(curatorFramework);
    }

    @Test
    @Order(value = 20)
    void testGetCuratorFrameworkForNamespace() {
        String namespace = "/testNamespace";
        CuratorFramework curatorFramework = zookeeperClient.getCuratorFrameworkForNamespace(namespace);
        assertNotNull(curatorFramework);
        assertEquals(ZookeeperClientConnection.getXynaFactoryZkNamespace() + namespace, curatorFramework.getNamespace());
    }

    @Test
    void testGetCuratorFrameworkForNamespaceWithoutSlash() {
        String namespace = "testNamespace";
        assertThrows(IllegalArgumentException.class, () -> {
            zookeeperClient.getCuratorFrameworkForNamespace(namespace);
        });
    }
}
