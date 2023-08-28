package com.gip.xyna.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public class VM_Zookeeper /* implements VetoManagementInterface */ {

    private static final String ZK_CONNECTION_STRING = "zookeeper:2181";
    private static final String XYNA_FACTORY_ZK_NAMESPACE = "com.gip.xyna.factory.distributed";
    private static final String VETO_PATH = "/vetos";
    private static final String LOCK_PATH = "/vetos.locks";
    private static final String VETO_BY_NAME = VETO_PATH + "/by-name";
    private static final String VETO_BY_ORDERID = VETO_PATH + "/by-orderid";

    private final AtomicBoolean isConnected = new AtomicBoolean(false);

    Logger log = LogManager.getLogger(this.getClass());

    CuratorFramework zkc;

    void init() {
        init(ZK_CONNECTION_STRING);
    }

    void init(String connectString) {
        Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.TRACE);

        zkc = CuratorFrameworkFactory.newClient(connectString,
                new ExponentialBackoffRetry(1000, 3));

        zkc.getConnectionStateListenable().addListener(new ConnectionStateListener() {

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                isConnected.set(newState.isConnected());
                if (log.isDebugEnabled())
                    log.debug("New connection state for Zookeeper client@"
                            + zkc.getZookeeperClient().getCurrentConnectionString() + ": " + newState.toString());
            }

        });

        zkc.start();
        if (log.isInfoEnabled()) {
            log.info("Connecting to Zookeeper: " + connectString);
        }
        InterProcessLock ipl = null;
        try {
            zkc.blockUntilConnected();
            if (!XYNA_FACTORY_ZK_NAMESPACE.equals(zkc.getNamespace())) {
                zkc = zkc.usingNamespace(XYNA_FACTORY_ZK_NAMESPACE);
                ipl = new InterProcessMutex(zkc, "/VM_Zookeeper Init Lock");
                if (ipl.acquire(0, TimeUnit.SECONDS)) {
                    if (zkc.checkExists().forPath(LOCK_PATH) == null) {
                        zkc.create().idempotent().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT)
                                .forPath(LOCK_PATH);
                    }
                    if (zkc.checkExists().forPath(VETO_BY_NAME) == null) {
                        zkc.create().idempotent().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT)
                                .forPath(VETO_BY_NAME);
                    }
                    if (zkc.checkExists().forPath(VETO_BY_ORDERID) == null) {
                        zkc.create().idempotent().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT)
                                .forPath(VETO_BY_ORDERID);
                    }
                }
            }
        } catch (java.lang.IllegalStateException e) {
            if (!zkc.getState().equals(CuratorFrameworkState.STARTED)) {
                log.error("Could not start Zookeeper client for Veto Management, state: " + zkc.getState());
                throw new RuntimeException(
                        "Could not start Zookeeper client for Veto Management, state: " + zkc.getState(), e);
            }
        } catch (Exception e) {
            log.error("Could not start Zookeeper client for Veto Management.");
            throw new RuntimeException("Could not start Zookeeper client for Veto Management.", e);
        } finally {
            if (ipl != null && ipl.isAcquiredInThisProcess())
                try {
                    ipl.release();
                } catch (Exception e) {
                }
        }
        if (log.isInfoEnabled()) {
            log.info("Connected to Zookeeper");
        }
    };

    private boolean reconnect() {
        try {
            if (zkc.getState().equals(CuratorFrameworkState.LATENT)) {
                zkc.start();
            }
        } catch (java.lang.IllegalStateException e) {
            if (!zkc.getState().equals(CuratorFrameworkState.STARTED)) {
                log.error("Could not start Zookeeper client for Veto Management, state: " + zkc.getState());
            }
        } catch (Exception e) {
            log.error("Could not start Zookeeper client for Veto Management.");
        }

        if (log.isDebugEnabled())
            log.debug("is connected: " + isConnected.get());

        return isConnected.get();
    }

    /* TODO uniqe idempotent transformation */
    private String sanitizeVeto(String veto) {
        // https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html#ch_zkDataModel
        // https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/main/java/org/apache/zookeeper/common/PathUtils.java#L43
        // and replace every /
        String sanitizedVeto = veto.replaceAll("[\u0000-\u001f\u007f-\u009F\ud800-\uf8ff\ufff0-\uffff/]", "_");

        if (log.isWarnEnabled() && !sanitizedVeto.equals(veto))
            log.warn("using sanitized Veto '" + sanitizedVeto + "' for Veto '" + veto + "'");

        return sanitizedVeto;
    }

    final ConcurrentHashMap<String, InterProcessMutex> vetoLocks = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, InterProcessMultiLock> vetoListLocks = new ConcurrentHashMap<>();

    private InterProcessMutex getOrCreateMutex(String veto) {

        String sanitizedVeto = sanitizeVeto(veto);

        InterProcessMutex vl = vetoLocks.get(sanitizedVeto);
        if (vl == null) {
            vl = new InterProcessMutex(zkc,
                    LOCK_PATH + "/" + sanitizedVeto);
            InterProcessMutex ipm = vetoLocks.putIfAbsent(sanitizedVeto, vl);
            if (ipm != null)
                vl = ipm;
        }

        return vl;
    }

    private InterProcessLock getLock(List<String> vetos) {

        if (vetos.size() > 1) {
            String vetoID = vetos.stream().map(v -> sanitizeVeto(v)).sorted().reduce("", String::concat);
            InterProcessMultiLock m = vetoListLocks.get(vetoID);

            if (m == null) {
                List<InterProcessLock> mutexes = new ArrayList<>(vetos.size());

                for (String v : vetos) {
                    mutexes.add(getOrCreateMutex(v));
                }
                m = new InterProcessMultiLock(mutexes);
                InterProcessMultiLock ipml = vetoListLocks.putIfAbsent(vetoID, m);
                if (ipml != null)
                    m = ipml;
            }

            return m;
        }

        if (vetos.size() == 1) {
            return getOrCreateMutex(vetos.get(0));
        }

        return null;
    }

    private byte[] serializeVetoInformation(VetoInformation vetoInformation) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {

            objectOutputStream.writeObject(vetoInformation);
            return byteArrayOutputStream.toByteArray();
        }
    }

    private VetoInformation deserializeVetoInformation(byte[] data) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
                ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {

            return (VetoInformation) objectInputStream.readObject();

        } catch (IOException | ClassNotFoundException e) {
            // Handle exceptions appropriately
            e.printStackTrace();
            return null;
        }
    }

    final ConcurrentHashMap<String, VetoInformation> vetoCache = new ConcurrentHashMap<>();

    /**
     * Versucht, die übergebenen Vetos für die angegebene OrderInformation zu
     * belegen.
     * Kann mehrfach gerufen werden, loggt dann aber eine Meldung.
     * 
     * @param orderInformation
     * @param vetos
     * @param urgency
     * @return
     */
    public VetoAllocationResult allocateVetos(OrderInformation orderInformation, List<String> vetos, long urgency) {

        if (vetos == null || vetos.size() <= 0)
            return VetoAllocationResult.SUCCESS;

        // check if another order holds one of the vetos
        Optional<String> existingVeto = vetos.stream()
                .map(v -> sanitizeVeto(v))
                .filter(v -> vetoCache.containsKey(v))
                .filter(v -> vetoCache.get(v).isAdministrative()
                        || !vetoCache.get(v).getUsingOrderId().equals(orderInformation.getOrderId()))
                .findFirst();

        if (existingVeto.isPresent()) {
            if (log.isDebugEnabled())
                log.debug("found other order holding veto in cache: " + vetoCache.get(existingVeto.get()));
            return new VetoAllocationResult(vetoCache.get(existingVeto.get()));
        }

        // don't block the scheduler if we have no connection to update the vetos
        if (!reconnect())
            return VetoAllocationResult.FAILED;

        InterProcessLock lock = getLock(vetos);

        boolean lockAquired = false;
        try {
            if (!(lockAquired = lock.acquire(0, TimeUnit.SECONDS))) { // tryLock
                return VetoAllocationResult.FAILED;
            }

            List<VetoInformation> existing = getExistingVetos(vetos);

            for (var vi : existing) {
                if (!vi.getUsingOrderId().equals(orderInformation.getOrderId()))
                    return new VetoAllocationResult(vi);
            }

            ArrayList<CuratorOp> operations;
            if (existing == null || existing.size() == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Allocating new vetos: " + vetos.stream().map(s -> s + " ").reduce("", String::concat));
                }
                operations = allocateVetoNodesForOrder(vetos, orderInformation);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Rellocating vetos: " + vetos.stream().map(s -> s + " ").reduce("", String::concat)
                            + " with existing "
                            + existing.stream().map(s -> s + " ").reduce("", String::concat));
                }
                operations = reallocateVetoNodesForOrder(vetos, orderInformation, existing);
            }

            if (operations == null)
                return VetoAllocationResult.FAILED;

            if (operations.isEmpty())
                return VetoAllocationResult.SUCCESS;

            // we might have lost the lock
            if (!reconnect() || !lock.isAcquiredInThisProcess())
                return VetoAllocationResult.FAILED;

            List<CuratorTransactionResult> results = zkc.transaction().forOperations(operations);

            if (checkTransactionResults(results)) {
                return VetoAllocationResult.SUCCESS;
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                if (lockAquired && lock.isAcquiredInThisProcess())
                    lock.release();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return VetoAllocationResult.FAILED;
    }

    private ArrayList<CuratorOp> reallocateVetoNodesForOrder(List<String> vetos, OrderInformation orderInformation,
            List<VetoInformation> existing)
            throws Exception {
        Set<String> existingSet = existing.stream().map(vi -> vi.getName()).collect(Collectors.toSet());
        List<String> newVetos = vetos.stream().filter(v -> !existingSet.contains(v)).collect(Collectors.toList());

        if (newVetos == null || newVetos.size() == 0)
            return Lists.newArrayList();

        return allocateVetoNodesForOrder(newVetos, orderInformation);
    }

    private ArrayList<CuratorOp> allocateVetoNodesForOrder(List<String> vetos, OrderInformation orderInformation)
            throws Exception {

        ArrayList<CuratorOp> operations = new ArrayList<>(vetos.size() * 2 + 1);

        String vetoNodeIdPath = VETO_BY_ORDERID + "/" + String.valueOf(orderInformation.getOrderId());
        if (zkc.checkExists().forPath(vetoNodeIdPath) == null) {
            CuratorOp opForId = zkc.transactionOp().create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(vetoNodeIdPath);

            operations.add(opForId);
        }

        for (String veto : vetos) {
            String sanitizedVeto = sanitizeVeto(veto);

            String vetoNodeNamePath = VETO_BY_NAME + "/" + sanitizedVeto;
            VetoInformation vi = new VetoInformation(veto, orderInformation, 0);
            byte[] orderInfoBytes = serializeVetoInformation(vi);
            CuratorOp opForName = zkc.transactionOp().create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(vetoNodeNamePath, orderInfoBytes);

            operations.add(opForName);

            CuratorOp opForId = zkc.transactionOp().create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(vetoNodeIdPath + "/" + sanitizedVeto);
            operations.add(opForId);
        }

        return operations;
    }

    private ArrayList<CuratorOp> deallocateVetoNodesForOrder(List<String> vetos, Long orderId)
            throws Exception {

        ArrayList<CuratorOp> operations = new ArrayList<>(vetos.size() * 2 + 1);

        String vetoNodeIdPath = VETO_BY_ORDERID + "/" + String.valueOf(orderId);
        List<String> allocatedVetos = zkc.getChildren().forPath(vetoNodeIdPath);

        for (String veto : vetos) {
            String sanitizedVeto = sanitizeVeto(veto);

            String vetoNodeNamePath = VETO_BY_NAME + "/" + sanitizedVeto;
            CuratorOp opForName = zkc.transactionOp().delete()
                    .forPath(vetoNodeNamePath);

            operations.add(opForName);

            CuratorOp opForId = zkc.transactionOp().delete()
                    .forPath(vetoNodeIdPath + "/" + sanitizedVeto);

            operations.add(opForId);
        }
        if (allocatedVetos == null || allocatedVetos.size() == vetos.size()) {
            CuratorOp opForId = zkc.transactionOp().delete()
                    .forPath(vetoNodeIdPath);
            operations.add(opForId);
        }

        return operations;
    }

    private boolean checkTransactionResults(List<CuratorTransactionResult> results) {
        for (CuratorTransactionResult result : results) {
            if (result.getError() != 0) {
                if (log.isWarnEnabled()) {
                    log.warn("got error " + result.getError() + " druring zookeeper transaction " + result.getType()
                            + " on " + result.getForPath());
                }
                return false;
            }
        }
        return true;
    }

    private List<VetoInformation> getAllExistingVetos() {
        List<VetoInformation> result = new ArrayList<>();

        String vetoNodePath = VETO_BY_NAME;
        Stat stat = null;
        try {
            stat = zkc.checkExists().forPath(vetoNodePath);
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<String> vetos = null;
        if (stat != null) {
            try {
                vetos = zkc.getChildren().forPath(vetoNodePath);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        if (vetos != null)
            for (String v : vetos) {
                try {
                    byte[] data = zkc.getData().forPath(vetoNodePath + "/" + v);
                    VetoInformation vi = deserializeVetoInformation(data);
                    result.add(vi);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        return result;
    }

    private List<VetoInformation> getExistingVetos(List<String> vetos) {
        List<VetoInformation> result = new ArrayList<>();

        for (String veto : vetos) {
            String vetoNodePath = VETO_BY_NAME + "/" + sanitizeVeto(veto);
            Stat stat = null;
            try {
                stat = zkc.checkExists().forPath(vetoNodePath);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            if (stat != null) {
                try {
                    byte[] data = zkc.getData().forPath(vetoNodePath);
                    VetoInformation vi = deserializeVetoInformation(data);
                    result.add(vi);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        return result;
    }

    private List<String> getExistingVetosForOrderId(Long orderId) {
        List<String> result = new ArrayList<>();

        String vetoNodePath = VETO_BY_ORDERID + "/" + String.valueOf(orderId);
        Stat stat = null;
        try {
            stat = zkc.checkExists().forPath(vetoNodePath);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (stat != null) {
            try {
                return zkc.getChildren().forPath(vetoNodePath);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return result;
    }

    final ConcurrentLinkedQueue<AbstractMap.SimpleEntry<OrderInformation, List<String>>> vetoProcessingQueue = new ConcurrentLinkedQueue<>();
    final ExecutorService vetoProcessor = Executors.newSingleThreadExecutor();

    /**
     * Macht die Belegungen des letzten allocateVetos(...) rückgängig.
     * 
     * @param orderInformation
     * @param vetos
     */
    public void undoAllocation(OrderInformation orderInformation, List<String> vetos) {
        // don't block the scheduler if we have no connection to update the vetos

        vetoProcessingQueue.add(new AbstractMap.SimpleEntry<OrderInformation, List<String>>(orderInformation,
                vetos));

        vetoProcessor.submit(() -> {

            AbstractMap.SimpleEntry<OrderInformation, List<String>> entry;

            while ((entry = vetoProcessingQueue.peek()) != null) {

                reconnect();

                InterProcessLock lock = getLock(entry.getValue());

                boolean lockAquired = false;
                try {

                    if (!(lockAquired = lock.acquire(500, TimeUnit.MILLISECONDS))) {
                        continue;
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Unallocating vetos: "
                                + entry.getValue().stream().map(s -> s + " ").reduce("", String::concat));
                    }

                    ArrayList<CuratorOp> operations = deallocateVetoNodesForOrder(entry.getValue(),
                            entry.getKey().getOrderId());

                    // we might have lost the lock
                    if (!reconnect() || !lock.isAcquiredInThisProcess())
                        continue;

                    List<CuratorTransactionResult> results = zkc.transaction().forOperations(operations);

                    if (checkTransactionResults(results)) {
                        vetoProcessingQueue.remove();
                        entry.getValue().forEach(v -> vetoCache.remove(v));
                    }

                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } finally {
                    try {
                        if (lockAquired && lock.isAcquiredInThisProcess())
                            lock.release();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        });

    }

    /**
     * Macht die Belegungen des letzten allocateVetos(...) permanent.
     * 
     * @param orderInformation
     * @param vetos
     */
    public void finalizeAllocation(OrderInformation orderInformation, List<String> vetos) {
        vetos.forEach(v -> vetoCache.putIfAbsent(v, new VetoInformation(sanitizeVeto(v), orderInformation, 0)));
    }

    /**
     * Versucht, die Vetos für die übergebene XynaOrder freizugeben.
     * Kann mehrfach gerufen werden, loggt dann aber eine Meldung.
     * 
     * @param orderInformation
     * @return false, wenn Vetos nicht oder von einem anderen Auftrag belegt waren
     */
    public boolean freeVetos(OrderInformation orderInformation) {

        reconnect();

        List<String> sanitizedVetos = getExistingVetosForOrderId(orderInformation.getOrderId());

        if (sanitizedVetos == null || sanitizedVetos.isEmpty())
            return false;

        undoAllocation(orderInformation, sanitizedVetos);

        return true;
    }

    /**
     * Versucht, die Vetos für die übergebene XynaOrder freizugeben.
     * Falls keine allokierten Vetos direkt gefunden werden, werden nochmal
     * alle Vetos durchsucht.
     * Kann mehrfach gerufen werden, loggt dann aber eine Meldung.
     * 
     * @param orderId
     * @return false, wenn Vetos nicht oder von einem anderen Auftrag belegt waren
     */
    public boolean freeVetosForced(long orderId) {
        boolean result = freeVetos(new OrderInformation(orderId, -1L, ""));

        if (result)
            return true;

        reconnect();

        List<VetoInformation> vi = getAllExistingVetos();

        if (vi == null || vi.isEmpty())
            return false;

        List<VetoInformation> vetosToProcess = vi.stream().filter(i -> i.getUsingOrderId().equals(orderId))
                .collect(Collectors.toList());

        if (vetosToProcess == null || vetosToProcess.isEmpty())
            return false;

        List<String> vetosToClear = vetosToProcess.stream().map(v -> v.getName()).collect(Collectors.toList());

        InterProcessLock lock = getLock(vetosToClear);

        try {
            lock.acquire();
            List<String> currVeto = new ArrayList<String>(1);
            for (var v : vetosToClear) {
                currVeto.add(v);
                try {
                    ArrayList<CuratorOp> operations = deallocateVetoNodesForOrder(currVeto, orderId);
                    zkc.transaction().forOperations(operations);
                } catch (Exception e) {
                }
                currVeto.clear();
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

            return false;
        } finally {
            try {
                if (lock.isAcquiredInThisProcess())
                    lock.release();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return true;
    }

    /**
     * Setzt ein administratives Veto
     * 
     * @param administrativeVeto
     * @throws XPRC_AdministrativeVetoAllocationDenied
     * @throws PersistenceLayerException
     */
    public void allocateAdministrativeVeto(AdministrativeVeto administrativeVeto)
    /* throws XPRC_AdministrativeVetoAllocationDenied, PersistenceLayerException */ {
        VetoInformation vi = new VetoInformation(administrativeVeto, 0);

        VetoAllocationResult result = allocateVetos(vi.getOrderInformation(), Arrays.asList(vi.getName()), 0);

        if (!result.isAllocated()) {
            // throw new XPRC_AdministrativeVetoAllocationDenied(existing.getName(),
            // existing.getUsingOrderId());
        }

        finalizeAllocation(vi.getOrderInformation(), Arrays.asList(vi.getName()));
    }

    /**
     * Ãndert Dokumentation eines administrativen Vetos, gibt alte Dokumentation
     * zurÃŒck
     * 
     * @param administrativeVeto
     * @return
     * @throws PersistenceLayerException
     * @throws XNWH_OBJECT_NOT_FOUND_FOR_PRIMARY_KEY
     */
    public String setDocumentationOfAdministrativeVeto(AdministrativeVeto administrativeVeto)
    /* throws PersistenceLayerException, XNWH_OBJECT_NOT_FOUND_FOR_PRIMARY_KEY */ {
        List<VetoInformation> lvi = getExistingVetos(Arrays.asList(administrativeVeto.getName()));

        if (lvi == null || lvi.isEmpty()) {
            // throw XNWH_OBJECT_NOT_FOUND_FOR_PRIMARY_KEY
            return "";
        }

        VetoInformation vi = lvi.get(0);

        String oldDoc = vi.getDocumentation();
        vi.setDocumentation(administrativeVeto.getDocumentation());

        try {
            zkc.setData().forPath(VETO_BY_NAME + "/" + sanitizeVeto(administrativeVeto.getName()),
                    serializeVetoInformation(vi));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        vetoCache.put(sanitizeVeto(vi.getName()), vi);

        return oldDoc;
    }

    /**
     * Entfernt ein administratives Veto, gibt entferntes Veto zurÃŒck
     * 
     * @param administrativeVeto
     * @return
     * @throws XPRC_AdministrativeVetoDeallocationDenied
     * @throws PersistenceLayerException
     */
    public VetoInformation freeAdministrativeVeto(AdministrativeVeto administrativeVeto)
    /*
     * throws XPRC_AdministrativeVetoDeallocationDenied, PersistenceLayerException
     */ {
        List<VetoInformation> lvi = getExistingVetos(Arrays.asList(administrativeVeto.getName()));

        if (lvi == null || lvi.isEmpty()) {
            // throw XNWH_OBJECT_NOT_FOUND_FOR_PRIMARY_KEY
            return new VetoInformation(administrativeVeto, 0);
        }

        VetoInformation vi = lvi.get(0);

        undoAllocation(vi.getOrderInformation(), Arrays.asList(vi.getName()));

        vetoCache.remove(vi.getName());

        return vi;
    }

    /**
     * Anzeige aller gesetzten Vetos
     * 
     * @return
     */
    public Collection<VetoInformation> listVetos() {
        // FIXME: Tests fail currently, because we don't block, when releasing vetos
        if (!vetoProcessingQueue.isEmpty()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return Collections.unmodifiableCollection(getAllExistingVetos());
    }

    /**
     * TODO wofÃŒr?
     * 
     * @param select
     * @param maxRows
     * @return
     * @throws PersistenceLayerException
     */
    /*
     * public VetoSearchResult searchVetos(VetoSelectImpl select, int maxRows)
     * throws PersistenceLayerException {
     * }
     */
    /**
     * Gibt VetoManagementAlgorithmType zurück
     * 
     * @return
     */

    /*
     * public VetoManagementAlgorithmType getAlgorithmType() {
     * }
     */
    /**
     * Ausgabe in CLI listExtendedSchedulerInfo
     * 
     * @return
     */
    public String showInformation() {
        // return getAlgorithmType() + ":
        // "+getAlgorithmType().getDocumentation().get(DocumentationLanguage.EN)+" Cache
        // size "+vetoCache.size();
        return "tbd";
    }

}
