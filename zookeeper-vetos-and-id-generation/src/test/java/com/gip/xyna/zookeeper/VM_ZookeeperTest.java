package com.gip.xyna.zookeeper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class VM_ZookeeperTest extends BaseClassForTests {

    private static String XYNA_FACTORY_ZK_NAMESPACE = "/com.gip.xyna.factory.distributed";
    private static String VETO_BY_NAME = XYNA_FACTORY_ZK_NAMESPACE + "/vetos/by-name";
    private static String VETO_BY_ORDERID = XYNA_FACTORY_ZK_NAMESPACE + "/vetos/by-orderid";

    private CuratorFramework client;

    @BeforeEach
    void setUpClient() {
        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(500));
        client.start();
    }

    @AfterEach
    void stop() {
        client.close();
    }

    @Test
    public void allocateVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);

        assertTrue(r.isAllocated());

        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 1").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 1").getVersion());

    }

    @Test
    public void allocateVetoWithXynaConnection() {
        VM_Zookeeper vm = new VM_Zookeeper();

        ZookeeperClientConnection.setConnectString(server.getConnectString());
        ZookeeperClientConnection zkc = ZookeeperClientConnection.getInstance();

        vm.init(zkc.getCuratorFramework());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);

        assertTrue(r.isAllocated());

        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 1").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 1").getVersion());

    }

    @Test
    public void allocateVetoWithSpecialChars() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r1 = vm.allocateVetos(oi1, Arrays.asList("Veto /"), 0);
        VetoAllocationResult r2 = vm.allocateVetos(oi1, Arrays.asList("Veto \ud800"), 0);
        VetoAllocationResult r3 = vm.allocateVetos(oi1,
                Arrays.asList("Veto äÄöÖüÜß :-D/\ud83d\ude00 -_.#+*,!?§$%&()[]{}<>|:=\"'\\"), 0);

        assertTrue(r1.isAllocated());
        assertDoesNotThrow(
                () -> client.checkExists().forPath(VETO_BY_NAME + "/Veto??_AsBase64URL_VmV0byAv").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto??_AsBase64URL_VmV0byAv")
                .getVersion());

        assertTrue(r2.isAllocated());
        assertNull(r2.getExistingVeto());
        assertDoesNotThrow(
                () -> client.checkExists().forPath(VETO_BY_NAME + "/Veto??_AsBase64URL_VmV0byA_").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto??_AsBase64URL_VmV0byA_")
                .getVersion());

        assertTrue(r3.isAllocated());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME
                + "/Veto???????????D????_?????????????????????????_AsBase64URL_VmV0byDDpMOEw7bDlsO8w5zDnyA6LUQv8J-YgCAtXy4jKyosIT_CpyQlJigpW117fTw-fDo9Iidc")
                .getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1"
                + "/Veto???????????D????_?????????????????????????_AsBase64URL_VmV0byDDpMOEw7bDlsO8w5zDnyA6LUQv8J-YgCAtXy4jKyosIT_CpyQlJigpW117fTw-fDo9Iidc")
                .getVersion());

        assertEquals(3, vm.listVetos().size());
    }

    @Test
    public void allocateMultipleVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1", "Veto 2"), 0);

        assertTrue(r.isAllocated());

        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 1").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 1").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 2").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 2").getVersion());

        assertTrue(vm.listVetos().size() == 2);
    }

    @Test
    public void allocateMultipleVetoSequential() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);

        assertTrue(r.isAllocated());
        assertTrue(vm.listVetos().size() == 1);
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 1").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 1").getVersion());

        r = vm.allocateVetos(oi1, Arrays.asList("Veto 2"), 0);

        assertTrue(r.isAllocated());
        assertTrue(vm.listVetos().size() == 2);
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 2").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 2").getVersion());
    }

    @Test
    public void listVetos() {

        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        assertTrue(vm.listVetos().isEmpty());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);

        assertTrue(vm.listVetos().size() == 1);

        OrderInformation oi2 = new OrderInformation(2L, 1L, "Type 2");
        vm.allocateVetos(oi2, Arrays.asList("Veto 2"), 0);

        assertTrue(vm.listVetos().size() == 2);

        vm.undoAllocation(oi1, Arrays.asList("Veto 1"));

        assertTrue(vm.listVetos().size() == 1);

        vm.undoAllocation(oi2, Arrays.asList("Veto 2"));

        assertTrue(vm.listVetos().isEmpty());

        vm.allocateVetos(oi1, Arrays.asList("Veto 1", "Veto 2"), 0);

        assertTrue(vm.listVetos().size() == 2);

        vm.undoAllocation(oi1, Arrays.asList("Veto 1"));

        assertTrue(vm.listVetos().size() == 1);

        vm.undoAllocation(oi1, Arrays.asList("Veto 2"));

        assertTrue(vm.listVetos().isEmpty());
    }

    @Test
    public void unallocateVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);

        assertTrue(r.isAllocated());

        vm.undoAllocation(oi1, Arrays.asList("Veto 1"));

        assertTrue(vm.listVetos().isEmpty());
        assertThrows(NullPointerException.class,
                () -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 1").getVersion());
        assertThrows(NullPointerException.class,
                () -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 1").getVersion());
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_NAME).isEmpty()));
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_ORDERID).isEmpty()));

        r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);
        assertTrue(r.isAllocated());
    }

    @Test
    public void freeVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);

        assertTrue(r.isAllocated());

        assertTrue(vm.freeVetos(oi1));

        assertTrue(vm.listVetos().isEmpty());
        assertThrows(NullPointerException.class,
                () -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 1").getVersion());
        assertThrows(NullPointerException.class,
                () -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 1").getVersion());
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_NAME).isEmpty()));
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_ORDERID).isEmpty()));

        assertFalse(vm.freeVetos(oi1));

        OrderInformation oi2 = new OrderInformation(2L, 2L, "Type 2");
        assertFalse(vm.freeVetos(oi2));
    }

    @Test
    public void freeVetoWithSpecialChars() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto /"), 0);

        assertTrue(r.isAllocated());

        assertTrue(vm.freeVetos(oi1));

        assertTrue(vm.listVetos().isEmpty());
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_NAME).isEmpty()));
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_ORDERID).isEmpty()));

        assertFalse(vm.freeVetos(oi1));
    }

    @Test
    public void freeVetos() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1", "Veto 2"), 0);

        assertTrue(r.isAllocated());

        assertTrue(vm.freeVetos(oi1));

        assertTrue(vm.listVetos().isEmpty());
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_NAME).isEmpty()));
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_ORDERID).isEmpty()));

        assertFalse(vm.freeVetos(oi1));
    }

    @Test
    public void freeVetoForced() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);

        assertTrue(r.isAllocated());

        assertTrue(vm.freeVetosForced(oi1.getOrderId()));

        assertTrue(vm.listVetos().isEmpty());
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_NAME).isEmpty()));
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_ORDERID).isEmpty()));

        assertFalse(vm.freeVetosForced(oi1.getOrderId()));

        OrderInformation oi2 = new OrderInformation(2L, 2L, "Type 2");
        assertFalse(vm.freeVetosForced(oi2.getOrderId()));
    }

    @Test
    public void freeVetoForcedWithSpecialChars() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto /"), 0);

        assertTrue(r.isAllocated());

        assertTrue(vm.freeVetosForced(oi1.getOrderId()));

        assertTrue(vm.listVetos().isEmpty());
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_NAME).isEmpty()));
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_ORDERID).isEmpty()));

        assertFalse(vm.freeVetosForced(oi1.getOrderId()));
    }

    @Test
    public void freeVetosForced() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1", "Veto 2"), 0);

        assertTrue(r.isAllocated());

        assertTrue(vm.freeVetosForced(oi1.getOrderId()));

        assertTrue(vm.listVetos().isEmpty());
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_NAME).isEmpty()));
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_ORDERID).isEmpty()));

        assertFalse(vm.freeVetosForced(oi1.getOrderId()));
    }

    @Test
    public void unallocateMultipleVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1", "Veto 2"), 0);

        assertTrue(r.isAllocated());

        vm.undoAllocation(oi1, Arrays.asList("Veto 1", "Veto 2"));

        assertTrue(vm.listVetos().isEmpty());
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_NAME).isEmpty()));
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_ORDERID).isEmpty()));
    }

    @Test
    public void unallocateMultipleVetoSequential() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1", "Veto 2"), 0);

        assertTrue(r.isAllocated());

        vm.undoAllocation(oi1, Arrays.asList("Veto 1"));

        assertTrue(vm.listVetos().size() == 1);
        assertThrows(NullPointerException.class,
                () -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 1").getVersion());
        assertThrows(NullPointerException.class,
                () -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 1").getVersion());

        vm.undoAllocation(oi1, Arrays.asList("Veto 2"));

        assertTrue(vm.listVetos().isEmpty());

        assertThrows(NullPointerException.class,
                () -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 2").getVersion());
        assertThrows(NullPointerException.class,
                () -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 2").getVersion());

        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_NAME).isEmpty()));
        assertDoesNotThrow(() -> assertTrue(client.getChildren().forPath(VETO_BY_ORDERID).isEmpty()));
    }

    @Test
    public void tryAllocateVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);

        assertTrue(r.isAllocated());

        OrderInformation oi2 = new OrderInformation(2L, 1L, "Type 2");
        r = vm.allocateVetos(oi2, Arrays.asList("Veto 1"), 0);
        assertFalse(r.isAllocated());
        assertEquals("Veto 1", r.getVetoName());
        assertThrows(NullPointerException.class,
                () -> client.checkExists().forPath(VETO_BY_ORDERID + "2" + "/Veto 1").getVersion());
        assertThrows(NullPointerException.class,() -> client.checkExists().forPath(VETO_BY_ORDERID + "/2").getVersion());
    }

    @Test
    public void tryAllocateMultipleVetos() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);

        assertTrue(r.isAllocated());

        OrderInformation oi2 = new OrderInformation(2L, 1L, "Type 2");
        r = vm.allocateVetos(oi2, Arrays.asList("Veto 1", "Veto 2"), 0);
        assertFalse(r.isAllocated());
        assertEquals("Veto 1", r.getVetoName());
        assertTrue(vm.listVetos().size() == 1);
        assertThrows(NullPointerException.class,
                () -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 2").getVersion());
        assertThrows(NullPointerException.class,
                () -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 2").getVersion());
        assertThrows(NullPointerException.class,() -> client.checkExists().forPath(VETO_BY_ORDERID + "/2").getVersion());
}

    @Test
    public void tryAllocateVetoToMultiple() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1", "Veto 2"), 0);

        assertTrue(r.isAllocated());

        OrderInformation oi2 = new OrderInformation(2L, 1L, "Type 2");
        r = vm.allocateVetos(oi2, Arrays.asList("Veto 2"), 0);
        assertFalse(r.isAllocated());
        assertEquals("Veto 2", r.getVetoName());
        assertTrue(vm.listVetos().size() == 2);
        assertThrows(NullPointerException.class,() -> client.checkExists().forPath(VETO_BY_ORDERID + "/2").getVersion());
    }

    @Test
    public void reallocateVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);

        assertTrue(r.isAllocated());

        r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);
        assertTrue(r.isAllocated());

        assertTrue(vm.listVetos().size() == 1);

        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 1").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 1").getVersion());
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    public void rereallocateVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");

        VetoAllocationResult r;
        for (int i = 1; i < 11; ++i) {
            r = vm.allocateVetos(oi1, Arrays.asList("Veto " + String.valueOf(i)), 0);
            assertTrue(r.isAllocated());
        }

        for (int i = 10; i > 0; --i) {
            vm.undoAllocation(oi1, Arrays.asList("Veto " + String.valueOf(i)));
        }

        r = vm.allocateVetos(oi1, Arrays.asList("Veto 1", "Veto 2"), 0);
        while (r.getExistingVeto() != null || VetoAllocationResult.FAILED.equals(r)) {
            r = vm.allocateVetos(oi1, Arrays.asList("Veto 1", "Veto 2"), 0);
        }
        assertTrue(r.isAllocated());

        while (vm.listVetos().size() != 2)
            ;
    }

    @Test
    public void addAllocationOfVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);

        assertTrue(r.isAllocated());

        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 1").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 1").getVersion());

        r = vm.allocateVetos(oi1, Arrays.asList("Veto 2"), 0);
        assertTrue(r.isAllocated());

        assertTrue(vm.listVetos().size() == 2);

        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 1").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 1").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 2").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 2").getVersion());
    }

    @Test
    public void removeAllocationOfVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1", "Veto 2"), 0);

        assertTrue(r.isAllocated());

        vm.undoAllocation(oi1, Arrays.asList("Veto 1"));

        assertTrue(vm.listVetos().size() == 1);
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 2").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1" + "/Veto 2").getVersion());
        assertThrows(NullPointerException.class,() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 1").getVersion());

        vm.undoAllocation(oi1, Arrays.asList("Veto 2"));

        assertTrue(vm.listVetos().isEmpty());
        assertThrows(NullPointerException.class,() -> client.checkExists().forPath(VETO_BY_NAME + "/Veto 2").getVersion());
        assertThrows(NullPointerException.class,() -> client.checkExists().forPath(VETO_BY_ORDERID + "/1").getVersion());
    }

    @Test
    public void allocateAdministrativeVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        vm.allocateAdministrativeVeto(new AdministrativeVeto("Test Admin Veto", "Test Doku"));

        assertTrue(vm.listVetos().size() == 1);
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Test Admin Veto").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/-1" + "/Test Admin Veto").getVersion());
    }

    @Test
    public void documentAdministrativeVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        var av = new AdministrativeVeto("Test Admin Veto", "Test Doku");
        vm.allocateAdministrativeVeto(av);

        assertTrue(vm.listVetos().size() == 1);

        av = new AdministrativeVeto("Test Admin Veto", "Test Doku 2");
        vm.setDocumentationOfAdministrativeVeto(av);

        assertTrue(vm.listVetos().size() == 1);
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_NAME + "/Test Admin Veto").getVersion());
        assertDoesNotThrow(() -> client.checkExists().forPath(VETO_BY_ORDERID + "/-1" + "/Test Admin Veto").getVersion());
    }

    @Test
    public void unallocateAdministrativeVeto() {
        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        var av = new AdministrativeVeto("Test Admin Veto", "Test Doku");
        vm.allocateAdministrativeVeto(av);

        assertTrue(vm.listVetos().size() == 1);

        vm.freeAdministrativeVeto(av);

        assertTrue(vm.listVetos().isEmpty());

        assertThrows(NullPointerException.class,() -> client.checkExists().forPath(VETO_BY_NAME + "/Test Admin Veto").getVersion());
        assertThrows(NullPointerException.class,() -> client.checkExists().forPath(VETO_BY_ORDERID + "/-1").getVersion());
    }

    @Test
    public void timeAllocateVeto() {

        int numVetos = 1_000;

        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        List<OrderInformation> oi = LongStream.range(0, numVetos)
                .mapToObj(i -> new OrderInformation(i, i, UUID.randomUUID().toString())).collect(Collectors.toList());
        List<List<String>> vetos = LongStream.range(0, numVetos)
                .mapToObj(i -> Arrays.asList(UUID.randomUUID().toString())).collect(Collectors.toList());

        long start = System.currentTimeMillis();
        for (int i = 0; i < numVetos; ++i) {
            vm.allocateVetos(oi.get(i), vetos.get(i), 0);
        }
        long stop = System.currentTimeMillis();

        assertEquals(numVetos, vm.listVetos().size());

        long allocationTime = (stop - start) / numVetos;

        System.out.println("Time per allocation [ms]: " + allocationTime);

        assertTrue(allocationTime < 50L, () -> "Allocationtime " + allocationTime + " < 50ms");
    }

    @Test
    public void exhaustInternalStorage() {

        final int STORAGE_SIZE = 10_000;

        final int numVetos = STORAGE_SIZE + 1_000;

        VM_Zookeeper vm = new VM_Zookeeper();

        vm.init(server.getConnectString());

        List<OrderInformation> oi = LongStream.range(0, numVetos)
                .mapToObj(i -> new OrderInformation(i, i, UUID.randomUUID().toString())).collect(Collectors.toList());
        List<List<String>> vetos = LongStream.range(0, numVetos)
                .mapToObj(i -> Arrays.asList(UUID.randomUUID().toString() + "öäß :-D/\ud83d\ude00"))
                .collect(Collectors.toList());

        for (int i = 0; i < numVetos; ++i) {
            VetoAllocationResult r = vm.allocateVetos(oi.get(i), vetos.get(i), 0);
            assertTrue(r.isAllocated());
        }

        assertEquals(numVetos, vm.listVetos().size());

        vm.shutdown();
    }
}
