package com.gip.xyna.zookeeper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.Test;

public class IDGeneratorTest extends BaseClassForTests {

    // see IDGenerationAlgorithmUsingZookeeper
    private static Long DEFAULT_INCREMENT = 50_000L;

    final int cores = Runtime.getRuntime().availableProcessors();

    final int threads = Math.min(2 * cores, 5);

    @Test
    public void testInit() throws Exception {

        IDGenerationAlgorithmUsingZookeeper idgen = new IDGenerationAlgorithmUsingZookeeper();

        idgen.init(server.getConnectString());

        assertEquals(10_001L, idgen.getUniqueId("default"));
        assertEquals(1L, idgen.getUniqueId("Test Realm"));
    }

    @Test
    public void testReInit() throws Exception {

        IDGenerationAlgorithmUsingZookeeper idgen = new IDGenerationAlgorithmUsingZookeeper();

        idgen.init(server.getConnectString());
        assertEquals(10_001L, idgen.getUniqueId("default"));
        assertEquals(1L, idgen.getUniqueId("Test Realm"));

        idgen.shutdown();

        idgen.init(server.getConnectString());
        assertEquals(10_002L, idgen.getUniqueId("default"));
        assertEquals(2L, idgen.getUniqueId("Test Realm"));

        idgen.shutdown();

        idgen = new IDGenerationAlgorithmUsingZookeeper();

        idgen.init(server.getConnectString());
        assertEquals(60_001L, idgen.getUniqueId("default"));
        assertEquals(50_001L, idgen.getUniqueId("Test Realm"));

    }

    @Test
    public void testServerRestart() throws Exception {

        IDGenerationAlgorithmUsingZookeeper idgen = new IDGenerationAlgorithmUsingZookeeper();

        idgen.init(server.getConnectString());
        assertEquals(10_001L, idgen.getUniqueId("default"));
        assertEquals(1L, idgen.getUniqueId("Test Realm"));

        server.restart();

        assertEquals(10_002L, idgen.getUniqueId("default"));
        assertEquals(2L, idgen.getUniqueId("Test Realm"));

    }

    @Test
    public void testServerStopped() throws Exception {

        IDGenerationAlgorithmUsingZookeeper idgen = new IDGenerationAlgorithmUsingZookeeper();

        idgen.init(server.getConnectString());

        Long counter = idgen.getUniqueId("default"); // 10_001
        server.stop();

        for (int i = 2; i < DEFAULT_INCREMENT; ++i) {
            counter = idgen.getUniqueId("default");
        }

        assertEquals(59_999L, counter);

    }

    private Long countByIncrements(IDGenerationAlgorithmUsingZookeeper idgen, int n) {
        for (int i = 1; i < n * DEFAULT_INCREMENT; ++i) {
            idgen.getUniqueId("Counter Realm");
        }

        return idgen.getUniqueId("Counter Realm");
    }

    @Test
    public void testParallelCounting() {

        final int increments = 2;

        IDGenerationAlgorithmUsingZookeeper idgen = new IDGenerationAlgorithmUsingZookeeper();

        idgen.init(server.getConnectString());

        Callable<Long> task = () -> {
            return countByIncrements(idgen, increments);
        };

        List<Callable<Long>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        try {
            List<Future<Long>> result = exec.invokeAll(tasks);

            var max = result.stream().map(f -> {
                try {
                    return f.get();
                } catch (InterruptedException e) {

                    e.printStackTrace();
                    return 1L;
                } catch (ExecutionException e) {

                    e.printStackTrace();
                    return 1L;
                }

            }).reduce(Math::max);

            assertEquals(DEFAULT_INCREMENT * threads * increments, max.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }

    }

    @Test
    public void testParallelAccess() {

        final int increments = 2;

        Callable<Long> task = () -> {
            IDGenerationAlgorithmUsingZookeeper idgen = new IDGenerationAlgorithmUsingZookeeper();
            idgen.init(server.getConnectString());
            return countByIncrements(idgen, increments);
        };

        List<Callable<Long>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        try {
            List<Future<Long>> result = exec.invokeAll(tasks);

            var max = result.stream().map(f -> {
                try {
                    return f.get() % DEFAULT_INCREMENT;
                } catch (InterruptedException e) {

                    e.printStackTrace();
                    return 1L;
                } catch (ExecutionException e) {

                    e.printStackTrace();
                    return 1L;
                }

            }).reduce(Math::max);

            assertEquals(0, max.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }

    }

    @Test
    public void testParallelAccessWithCluster() {

        final int increments = 2;

        TestingCluster cluster = null;
        try {
            cluster = createAndStartCluster(5);
            cluster.start();
        } catch (Exception e) {

            e.printStackTrace();
        }

        assertNotNull(cluster);
        final String connecString = cluster.getConnectString();

        Callable<Long> task = () -> {
            IDGenerationAlgorithmUsingZookeeper idgen = new IDGenerationAlgorithmUsingZookeeper();
            idgen.init(connecString);
            return countByIncrements(idgen, increments);
        };

        List<Callable<Long>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        try {
            List<Future<Long>> result = exec.invokeAll(tasks);

            var max = result.stream().map(f -> {
                try {
                    return f.get() % DEFAULT_INCREMENT;
                } catch (InterruptedException e) {

                    e.printStackTrace();
                    return 1L;
                } catch (ExecutionException e) {

                    e.printStackTrace();
                    return 1L;
                }

            }).reduce(Math::max);

            assertEquals(0, max.get());
        } catch (InterruptedException e) {
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
    public void testParallelAccessWithDegradedCluster() {

        final int increments = 200;

        TestingCluster cluster = null;
        try {
            cluster = createAndStartCluster(5);
            cluster.start();
        } catch (Exception e) {

            e.printStackTrace();
        }

        assertNotNull(cluster);
        final String connecString = cluster.getConnectString();

        Callable<Long> task = () -> {
            IDGenerationAlgorithmUsingZookeeper idgen = new IDGenerationAlgorithmUsingZookeeper();
            idgen.init(connecString);
            return countByIncrements(idgen, increments);
        };

        List<Callable<Long>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        List<Future<Long>> result = tasks.stream().parallel().map(s -> exec.submit(s)).collect(Collectors.toList());

        try {
            var it = cluster.getInstances().iterator();
            assertTrue(cluster.killServer(it.next()));
            exec.shutdown();
            assertTrue(cluster.killServer(it.next()));
            assertTrue(exec.awaitTermination(60, TimeUnit.SECONDS));

            var max = result.stream().map(f -> {
                try {
                    return f.get() % DEFAULT_INCREMENT;
                } catch (InterruptedException e) {

                    e.printStackTrace();
                    return 1L;
                } catch (ExecutionException e) {

                    e.printStackTrace();
                    return 1L;
                }

            }).reduce(Math::max);

            assertEquals(0, max.get());
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
    public void testParallelAccessWithFailedCluster() {

        final int increments = 200;

        TestingCluster cluster = null;
        try {
            cluster = createAndStartCluster(5);
            cluster.start();
        } catch (Exception e) {

            e.printStackTrace();
        }

        assertNotNull(cluster);
        final String connecString = cluster.getConnectString();

        Callable<Long> task = () -> {
            IDGenerationAlgorithmUsingZookeeper idgen = new IDGenerationAlgorithmUsingZookeeper();
            idgen.init(connecString);
            return countByIncrements(idgen, increments);
        };

        List<Callable<Long>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        tasks.stream().parallel().map(s -> exec.submit(s)).collect(Collectors.toList());

        try {
            var it = cluster.getInstances().iterator();
            assertTrue(cluster.killServer(it.next()));
            exec.shutdown();
            assertTrue(cluster.killServer(it.next()));
            assertTrue(cluster.killServer(it.next()));
            assertFalse(exec.awaitTermination(60, TimeUnit.SECONDS));

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
    public void testParallelAccessWithRestartingCluster() {

        final int increments = 200;

        TestingCluster cluster = null;
        try {
            cluster = createAndStartCluster(5);
            cluster.start();
        } catch (Exception e) {

            e.printStackTrace();
        }

        assertNotNull(cluster);
        final String connecString = cluster.getConnectString();

        Callable<Long> task = () -> {
            IDGenerationAlgorithmUsingZookeeper idgen = new IDGenerationAlgorithmUsingZookeeper();
            idgen.init(connecString);
            return countByIncrements(idgen, increments);
        };

        List<Callable<Long>> tasks = Lists.newArrayList();
        for (int i = 0; i < threads; ++i)
            tasks.add(task);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        List<Future<Long>> result = tasks.stream().parallel().map(s -> exec.submit(s)).collect(Collectors.toList());

        try {
            cluster.stop();
            exec.shutdown();
            assertFalse(exec.awaitTermination(60, TimeUnit.SECONDS));
            cluster.getServers().forEach(s -> {
                try {
                    s.restart();
                } catch (Exception e) {
                }
            });
            assertTrue(exec.awaitTermination(60, TimeUnit.SECONDS));

            var max = result.stream().map(f -> {
                try {
                    return f.isCancelled() ? 1L : f.get() % DEFAULT_INCREMENT;
                } catch (InterruptedException e) {

                    e.printStackTrace();
                    return 1L;
                } catch (ExecutionException e) {

                    e.printStackTrace();
                    return 1L;
                }

            }).reduce(Math::max);

            assertEquals(0, max.get());

            max = result.stream().map(f -> {
                try {
                    return f.get();
                } catch (InterruptedException e) {

                    e.printStackTrace();
                    return 0L;
                } catch (ExecutionException e) {

                    e.printStackTrace();
                    return 0L;
                }

            }).reduce(Math::max);

            assertEquals(threads * increments * DEFAULT_INCREMENT, max.get());
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
}
