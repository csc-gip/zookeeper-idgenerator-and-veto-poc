package com.gip.xyna.zookeeper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.junit.jupiter.api.Test;

public class VM_ZookeeperThreadTest extends BaseClassForTests {
    
    final int cores = Runtime.getRuntime().availableProcessors();

    final int threads = Math.min(2 * cores, 5);
    final OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
    final OrderInformation oi2 = new OrderInformation(2L, 2L, "Type 2");
    final List<String> veto1 = Arrays.asList("Veto 1");
    final List<String> veto2 = Arrays.asList("Veto 2");
    final List<String> veto12 = Arrays.asList("Veto 1", "Veto 2");

    @Test
    public void multipleParallelAllocationsSingleInstance() {

        VM_Zookeeper vm = new VM_Zookeeper();
        vm.init(server.getConnectString());

        final AtomicLong counter = new AtomicLong(0);

        Callable<VetoAllocationResult> task = () -> {
            return vm.allocateVetos(
                    new OrderInformation(counter.incrementAndGet(), counter.incrementAndGet(),
                            String.valueOf(counter.incrementAndGet())),
                    veto1, 0);
        };

        List<Callable<VetoAllocationResult>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        try {
            List<Future<VetoAllocationResult>> results = exec.invokeAll(tasks);

            Integer allocatedVetos = results.stream().map(r -> {
                try {
                    return r.get().isAllocated() ? 1 : 0;
                } catch (Exception e) {
                    e.printStackTrace();
                    return Integer.MAX_VALUE - threads;
                }
            }).reduce(0, Math::addExact);
            assertEquals(1, allocatedVetos);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertTrue(vm.listVetos().size() == 1);

    }

    @Test
    public void multipleParallelAllocationsMultipleInstance() {

        final AtomicLong counter = new AtomicLong(0);
        final String connecString = server.getConnectString();

        Callable<VetoAllocationResult> task = () -> {
            VM_Zookeeper vm = new VM_Zookeeper();
            vm.init(connecString);
            return vm.allocateVetos(
                    new OrderInformation(counter.incrementAndGet(), counter.incrementAndGet(),
                            String.valueOf(counter.incrementAndGet())),
                    veto1, 0);
        };

        List<Callable<VetoAllocationResult>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        try {
            List<Future<VetoAllocationResult>> results = exec.invokeAll(tasks);

            Integer allocatedVetos = results.stream().map(r -> {
                try {
                    return r.get().isAllocated() ? 1 : 0;
                } catch (Exception e) {
                    e.printStackTrace();
                    return Integer.MAX_VALUE - threads;
                }
            }).reduce(0, Math::addExact);
            assertEquals(1, allocatedVetos);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void multipleParallelAllocationsMultipleInstanceOnCluster() {

        TestingCluster cluster = null;
        try {
            cluster = createAndStartCluster(5);
            cluster.start();
        } catch (Exception e) {

            e.printStackTrace();
        }

        assertNotNull(cluster);
        final String connecString = cluster.getConnectString();

        final AtomicLong counter = new AtomicLong(0);

        Callable<VetoAllocationResult> task = () -> {
            VM_Zookeeper vm = new VM_Zookeeper();
            vm.init(connecString);
            return vm.allocateVetos(
                    new OrderInformation(counter.incrementAndGet(), counter.incrementAndGet(),
                            String.valueOf(counter.incrementAndGet())),
                    veto1, 0);
        };

        List<Callable<VetoAllocationResult>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        try {
            List<Future<VetoAllocationResult>> results = exec.invokeAll(tasks);

            Integer allocatedVetos = results.stream().map(r -> {
                try {
                    return r.get().isAllocated() ? 1 : 0;
                } catch (Exception e) {
                    e.printStackTrace();
                    return Integer.MAX_VALUE - threads;
                }
            }).reduce(0, Math::addExact);
            assertEquals(1, allocatedVetos);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                cluster.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    @Test
    public void multipleParallelAllocationsMultipleInstanceOnDegradedCluster() {

        Timing timing = new Timing(60, TimeUnit.SECONDS);
        final int waiting_time = timing.session();

        TestingCluster cluster = null;
        try {
            cluster = createAndStartCluster(5);
            cluster.start();
        } catch (Exception e) {

            e.printStackTrace();
        }

        assertNotNull(cluster);
        final String connecString = cluster.getConnectString();

        final AtomicLong counter = new AtomicLong(0);

        Callable<VetoAllocationResult> task = () -> {
            VM_Zookeeper vm = new VM_Zookeeper();
            vm.init(connecString);
            Thread.sleep(waiting_time + 500);
            return vm.allocateVetos(
                    new OrderInformation(counter.incrementAndGet(), counter.incrementAndGet(),
                            String.valueOf(counter.incrementAndGet())),
                    veto1, 0);
        };

        List<Callable<VetoAllocationResult>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        List<Future<VetoAllocationResult>> results = tasks.stream().parallel().map(s -> exec.submit(s))
                .collect(Collectors.toList());

        try {
            var it = cluster.getInstances().iterator();
            assertTrue(cluster.killServer(it.next()));
            assertTrue(cluster.killServer(it.next()));
            exec.shutdown();
            assertTrue(exec.awaitTermination(waiting_time + 5000, TimeUnit.MILLISECONDS));

            Integer allocatedVetos = results.stream().map(r -> {
                try {
                    return r.get().isAllocated() ? 1 : 0;
                } catch (Exception e) {
                    e.printStackTrace();
                    return Integer.MAX_VALUE - threads;
                }
            }).reduce(0, Math::addExact);
            assertEquals(1, allocatedVetos);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            try {
                cluster.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    @Test
    public void multipleParallelAllocationsMultipleInstanceOnFailedCluster() {

        Timing timing = new Timing(60, TimeUnit.SECONDS);
        final int waiting_time = timing.session();

        TestingCluster cluster = null;
        try {
            cluster = createAndStartCluster(5);
            cluster.start();
        } catch (Exception e) {

            e.printStackTrace();
        }

        assertNotNull(cluster);
        final String connecString = cluster.getConnectString();

        final AtomicLong counter = new AtomicLong(0);

        Callable<VetoAllocationResult> task = () -> {
            VM_Zookeeper vm = new VM_Zookeeper();
            vm.init(connecString);
            Thread.sleep(waiting_time + 500);
            return vm.allocateVetos(
                    new OrderInformation(counter.incrementAndGet(), counter.incrementAndGet(),
                            String.valueOf(counter.incrementAndGet())),
                    veto1, 0);
        };

        List<Callable<VetoAllocationResult>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        List<Future<VetoAllocationResult>> results = tasks.stream().parallel().map(s -> exec.submit(s))
                .collect(Collectors.toList());

        try {
            Thread.sleep(waiting_time / 2);
            var it = cluster.getInstances().iterator();
            assertTrue(cluster.killServer(it.next()));
            assertTrue(cluster.killServer(it.next()));
            assertTrue(cluster.killServer(it.next()));

            exec.shutdown();
            assertTrue(exec.awaitTermination(waiting_time * 1000 + 5, TimeUnit.SECONDS));

            Integer allocatedVetos = results.stream().map(r -> {
                try {
                    return r.get().isAllocated() ? 1 : 0;
                } catch (Exception e) {
                    e.printStackTrace();
                    return Integer.MAX_VALUE - threads;
                }
            }).reduce(0, Math::addExact);
            assertEquals(0, allocatedVetos);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                cluster.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    @Test
    public void multipleParallelAllocationsMultipleInstanceOnRestartedCluster() {

        Timing timing = new Timing(60, TimeUnit.SECONDS);
        final int waiting_time = timing.session();

        TestingCluster cluster = null;
        try {
            cluster = createAndStartCluster(5);
            cluster.start();
        } catch (Exception e) {

            e.printStackTrace();
        }

        assertNotNull(cluster);
        final String connecString = cluster.getConnectString();

        final AtomicLong counter = new AtomicLong(0);

        Callable<VetoAllocationResult> task = () -> {
            VM_Zookeeper vm = new VM_Zookeeper();
            vm.init(connecString);
            Thread.sleep(2 * waiting_time + 500);
            return vm.allocateVetos(
                    new OrderInformation(counter.incrementAndGet(), counter.incrementAndGet(),
                            String.valueOf(counter.incrementAndGet())),
                    veto1, 0);
        };

        List<Callable<VetoAllocationResult>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        List<Future<VetoAllocationResult>> results = tasks.stream().parallel().map(s -> exec.submit(s))
                .collect(Collectors.toList());

        try {
            var it = cluster.getServers().iterator();
            timing.sleepABit();
            it.next().stop();
            it.next().stop();
            it.next().stop();

            timing.sleepABit();
            cluster.getServers().iterator().next().restart();

            exec.shutdown();
            assertTrue(exec.awaitTermination(2 * waiting_time * 1000 + 5, TimeUnit.SECONDS));

            Integer allocatedVetos = results.stream().map(r -> {
                try {
                    return r.get().isAllocated() ? 1 : 0;
                } catch (Exception e) {
                    e.printStackTrace();
                    return Integer.MAX_VALUE - threads;
                }
            }).reduce(0, Math::addExact);
            assertEquals(1, allocatedVetos);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                cluster.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    @Test
    public void multipleAllocationsSingleInstance() {

        VM_Zookeeper vm = new VM_Zookeeper();
        vm.init(server.getConnectString());

        final AtomicLong counter = new AtomicLong(0);

        List<String> vetos = Lists.newArrayList();
        for (int i = 0; i < 10; ++i)
            vetos.add(UUID.randomUUID().toString());

        Callable<Boolean> task = () -> {

            final List<String> unallocated = vetos;
            Set<String> allocated = new HashSet<String>();

            while (allocated.size() < unallocated.size())
                for (String veto : unallocated) {
                    OrderInformation oi = new OrderInformation(counter.incrementAndGet(), counter.incrementAndGet(),
                            String.valueOf(counter.incrementAndGet()));
                    if (!allocated.contains(veto)) {
                        VetoAllocationResult r = vm.allocateVetos(
                                oi,
                                Arrays.asList(veto), 0);
                        if (r.isAllocated()) {
                            allocated.add(veto);
                            Executors.newSingleThreadExecutor().submit(() -> {
                                try {
                                    Thread.sleep(500);
                                } catch (InterruptedException e) {
                                }
                                vm.undoAllocation(oi, Arrays.asList(veto));
                            });
                        }
                    }
                }

            return Boolean.TRUE;
        };

        List<Callable<Boolean>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        List<Future<Boolean>> results = tasks.stream().parallel().map(s -> exec.submit(s))
                .collect(Collectors.toList());

        exec.shutdown();
        try {
            assertTrue(exec.awaitTermination(threads * vetos.size() * 2 + 10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            exec.shutdownNow();
        }

        assertFalse(results.stream().anyMatch(b -> {
            try {
                return !b.get();
            } catch (Exception e) {
                return Boolean.TRUE;
            }
        }));

    }

    @Test
    public void multipleAllocationsMultipleInstance() {

        final AtomicLong counter = new AtomicLong(0);

        List<String> vetos = Lists.newArrayList();
        for (int i = 0; i < 10; ++i)
            vetos.add(UUID.randomUUID().toString());

        Callable<Boolean> task = () -> {

            VM_Zookeeper vm = new VM_Zookeeper();
            vm.init(server.getConnectString());

            final List<String> unallocated = vetos;
            Set<String> allocated = new HashSet<String>();

            while (allocated.size() < unallocated.size())
                for (String veto : unallocated) {
                    OrderInformation oi = new OrderInformation(counter.incrementAndGet(), counter.incrementAndGet(),
                            String.valueOf(counter.incrementAndGet()));
                    if (!allocated.contains(veto)) {
                        VetoAllocationResult r = vm.allocateVetos(
                                oi,
                                Arrays.asList(veto), 0);
                        if (r.isAllocated()) {
                            allocated.add(veto);
                            Executors.newSingleThreadExecutor().submit(() -> {
                                try {
                                    Thread.sleep(500);
                                } catch (InterruptedException e) {
                                }
                                vm.undoAllocation(oi, Arrays.asList(veto));
                            });
                        }
                    }
                }

            return Boolean.TRUE;
        };

        List<Callable<Boolean>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        List<Future<Boolean>> results = tasks.stream().parallel().map(s -> exec.submit(s))
                .collect(Collectors.toList());

        exec.shutdown();
        try {
            assertTrue(exec.awaitTermination(threads * vetos.size() * 2 + 10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            exec.shutdownNow();
        }

        assertFalse(results.stream().anyMatch(b -> {
            try {
                return !b.get();
            } catch (Exception e) {
                return Boolean.TRUE;
            }
        }));

    }


    @Test
    public void multipleAllocationsMultipleInstanceOnCluster() {

        TestingCluster cluster = null;
        try {
            cluster = createAndStartCluster(5);
            cluster.start();
        } catch (Exception e) {

            e.printStackTrace();
        }

        assertNotNull(cluster);
        final String connecString = cluster.getConnectString();

        final AtomicLong counter = new AtomicLong(0);

        List<String> vetos = Lists.newArrayList();
        for (int i = 0; i < 10; ++i)
            vetos.add(UUID.randomUUID().toString());

        Callable<Boolean> task = () -> {

            VM_Zookeeper vm = new VM_Zookeeper();
            vm.init(connecString);

            final List<String> unallocated = vetos;
            Set<String> allocated = new HashSet<String>();

            while (allocated.size() < unallocated.size())
                for (String veto : unallocated) {
                    OrderInformation oi = new OrderInformation(counter.incrementAndGet(), counter.incrementAndGet(),
                            String.valueOf(counter.incrementAndGet()));
                    if (!allocated.contains(veto)) {
                        VetoAllocationResult r = vm.allocateVetos(
                                oi,
                                Arrays.asList(veto), 0);
                        if (r.isAllocated()) {
                            allocated.add(veto);
                            Executors.newSingleThreadExecutor().submit(() -> {
                                try {
                                    Thread.sleep(500);
                                } catch (InterruptedException e) {
                                }
                                vm.undoAllocation(oi, Arrays.asList(veto));
                            });
                        }
                    }
                }

            return Boolean.TRUE;
        };

        List<Callable<Boolean>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        List<Future<Boolean>> results = tasks.stream().parallel().map(s -> exec.submit(s))
                .collect(Collectors.toList());

        exec.shutdown();
        try {
            assertTrue(exec.awaitTermination(threads * vetos.size() * 2 + 10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            exec.shutdownNow();
        }

        assertFalse(results.stream().anyMatch(b -> {
            try {
                return !b.get();
            } catch (Exception e) {
                return Boolean.TRUE;
            }
        }));

    }}