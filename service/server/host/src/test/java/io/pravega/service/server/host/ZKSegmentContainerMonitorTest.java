/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.service.server.host;

import io.pravega.common.cluster.Host;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.service.server.ContainerHandle;
import io.pravega.service.server.SegmentContainerRegistry;
import io.pravega.testcommon.AssertExtensions;
import io.pravega.testcommon.TestUtils;
import io.pravega.testcommon.TestingServerStarter;
import io.pravega.testcommon.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ZKSegmentContainerMonitorTest extends ThreadPooledTestSuite {
    private final static int TEST_TIMEOUT = 60000;
    private final static int RETRY_SLEEP_MS = 100;
    private final static int MAX_RETRY = 5;
    private static final int PORT = TestUtils.getAvailableListenPort();
    private final static Host PRAVEGA_SERVICE_ENDPOINT = new Host(getHostAddress(), PORT, null);
    private final static String PATH = ZKPaths.makePath("cluster", "segmentContainerHostMapping");
    private String zkUrl;

    private TestingServer zkTestServer;

    // Timeout per method tested.
    @Rule
    public Timeout globalTimeout = Timeout.millis(TEST_TIMEOUT);

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServerStarter().start();
        zkUrl = zkTestServer.getConnectString();
    }

    @After
    public void stopZookeeper() throws IOException {
        zkTestServer.close();
    }

    /**
     * Test if no mapping is present in zk.
     *
     * @throws Exception if an error occurred.
     */
    @Test
    public void testInitializeNoMapping() throws Exception {
        @Cleanup
        CuratorFramework zkClient = startClient();
        @Cleanup
        ZKSegmentContainerMonitor segMonitor = createContainerMonitor(createMockContainerRegistry(), zkClient);
        segMonitor.initialize();
        assertEquals("Unexpected number of handles.", 0, segMonitor.getRegisteredContainers().size());
    }

    /**
     * Tests if we cannot connect to ZooKeeper (the exception must be propagated to the caller).
     *
     * @throws Exception if an error occurred.
     */
    @Test
    public void testInitializeError() throws Exception {
        @Cleanup
        CuratorFramework zkClient = startClient();
        @Cleanup
        ZKSegmentContainerMonitor segMonitor = createContainerMonitor(createMockContainerRegistry(), zkClient);
        zkClient.close();

        AssertExtensions.assertThrows(
                "initialize() did not throw an exception when ZooKeeper could not be accessed.",
                () -> segMonitor.initialize(),
                ex -> true); // Any exception will do, as long as it is propagated.
    }

    @Test
    public void testStartAndStopContainer() throws Exception {
        @Cleanup
        CuratorFramework zkClient = startClient();
        initializeHostContainerMapping(zkClient);

        SegmentContainerRegistry containerRegistry = createMockContainerRegistry();
        @Cleanup
        ZKSegmentContainerMonitor segMonitor = createContainerMonitor(containerRegistry, zkClient);
        segMonitor.initialize(Duration.ofSeconds(1));

        // Simulate a container that starts successfully.
        CompletableFuture<ContainerHandle> startupFuture = new CompletableFuture<>();
        ContainerHandle containerHandle = mock(ContainerHandle.class);
        when(containerHandle.getContainerId()).thenReturn(2);
        when(containerRegistry.startContainer(eq(2), any()))
                .thenReturn(startupFuture);

        // Now modify the ZK entry.
        HashMap<Host, Set<Integer>> currentData = deserialize(zkClient, PATH);
        currentData.put(PRAVEGA_SERVICE_ENDPOINT, Collections.singleton(2));
        zkClient.setData().forPath(PATH, SerializationUtils.serialize(currentData));

        // Container finished starting.
        startupFuture.complete(containerHandle);
        verify(containerRegistry, timeout(10000).atLeastOnce()).startContainer(eq(2), any());

        Thread.sleep(2000);
        assertEquals(1, segMonitor.getRegisteredContainers().size());
        assertTrue(segMonitor.getRegisteredContainers().contains(2));

        // Now modify the ZK entry. Remove container 2 and add 1.
        HashMap<Host, Set<Integer>> newMapping = new HashMap<>();
        newMapping.put(PRAVEGA_SERVICE_ENDPOINT, Collections.singleton(1));
        zkClient.setData().forPath(PATH, SerializationUtils.serialize(newMapping));

        // Verify that stop is called and only the newly added container is in running state.
        when(containerRegistry.stopContainer(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        verify(containerRegistry, timeout(10000).atLeastOnce()).stopContainer(any(), any());

        // Using wait here to ensure the private data structure is updated.
        // TODO: Removing dependency on sleep here and other places in this class
        // - https://github.com/pravega/pravega/issues/1079
        Thread.sleep(2000);
        assertEquals(1, segMonitor.getRegisteredContainers().size());
        assertTrue(segMonitor.getRegisteredContainers().contains(1));
    }

    @Test
    public void testShutdownNotYetStartedContainer() throws Exception {
        @Cleanup
        CuratorFramework zkClient = startClient();
        initializeHostContainerMapping(zkClient);

        SegmentContainerRegistry containerRegistry = createMockContainerRegistry();
        @Cleanup
        ZKSegmentContainerMonitor segMonitor = createContainerMonitor(containerRegistry, zkClient);
        segMonitor.initialize(Duration.ofSeconds(1));

        // Simulate a container that takes a long time to start. Should be greater than a few monitor loops.
        ContainerHandle containerHandle = mock(ContainerHandle.class);
        when(containerHandle.getContainerId()).thenReturn(2);
        CompletableFuture<ContainerHandle> startupFuture = FutureHelpers.delayedFuture(
                () -> CompletableFuture.completedFuture(containerHandle), 3000, executorService());
        when(containerRegistry.startContainer(eq(2), any()))
                .thenReturn(startupFuture);

        // Use ZK to send that information to the Container Manager.
        HashMap<Host, Set<Integer>> currentData = deserialize(zkClient, PATH);
        currentData.put(PRAVEGA_SERVICE_ENDPOINT, Collections.singleton(2));
        zkClient.setData().forPath(PATH, SerializationUtils.serialize(currentData));

        // Verify it's not yet started.
        verify(containerRegistry, timeout(10000).atLeastOnce()).startContainer(eq(2), any());
        assertEquals(0, segMonitor.getRegisteredContainers().size());

        // Now simulate shutting it down.
        when(containerRegistry.stopContainer(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        currentData.clear();
        zkClient.setData().forPath(PATH, SerializationUtils.serialize(currentData));

        verify(containerRegistry, timeout(10000).atLeastOnce()).stopContainer(any(), any());
        Thread.sleep(2000);
        assertEquals(0, segMonitor.getRegisteredContainers().size());
    }

    @Test
    public void testRetryOnStartFailures() throws Exception {
        @Cleanup
        CuratorFramework zkClient = startClient();
        initializeHostContainerMapping(zkClient);

        SegmentContainerRegistry containerRegistry = createMockContainerRegistry();
        @Cleanup
        ZKSegmentContainerMonitor segMonitor = createContainerMonitor(containerRegistry, zkClient);
        segMonitor.initialize(Duration.ofSeconds(1));

        // Simulate a container that fails to start.
        CompletableFuture<ContainerHandle> failedFuture = FutureHelpers.failedFuture(new RuntimeException());
        when(containerRegistry.startContainer(eq(2), any()))
                .thenReturn(failedFuture);

        // Use ZK to send that information to the Container Manager.
        HashMap<Host, Set<Integer>> currentData = deserialize(zkClient, PATH);
        currentData.put(PRAVEGA_SERVICE_ENDPOINT, Collections.singleton(2));
        zkClient.setData().forPath(PATH, SerializationUtils.serialize(currentData));

        // Verify that it does not start.
        verify(containerRegistry, timeout(10000).atLeastOnce()).startContainer(eq(2), any());
        assertEquals(0, segMonitor.getRegisteredContainers().size());

        // Now simulate success for the same container.
        ContainerHandle containerHandle = mock(ContainerHandle.class);
        when(containerHandle.getContainerId()).thenReturn(2);
        when(containerRegistry.startContainer(eq(2), any()))
                .thenReturn(CompletableFuture.completedFuture(containerHandle));

        // Verify that it retries and starts the same container again.
        verify(containerRegistry, timeout(10000).atLeastOnce()).startContainer(eq(2), any());
        Thread.sleep(2000);
        assertEquals(1, segMonitor.getRegisteredContainers().size());
    }

    @Test
    public void testClose() throws Exception {
        @Cleanup
        CuratorFramework zkClient = startClient();
        initializeHostContainerMapping(zkClient);

        SegmentContainerRegistry containerRegistry = mock(SegmentContainerRegistry.class);
        ContainerHandle containerHandle1 = mock(ContainerHandle.class);
        when(containerHandle1.getContainerId()).thenReturn(1);
        when(containerRegistry.startContainer(eq(1), any()))
                .thenReturn(CompletableFuture.completedFuture(containerHandle1));
        when(containerRegistry.stopContainer(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        ZKSegmentContainerMonitor segMonitor = createContainerMonitor(containerRegistry, zkClient);

        segMonitor.initialize(Duration.ofSeconds(1));

        segMonitor.close();
        assertEquals(0, segMonitor.getRegisteredContainers().size());
    }

    @SneakyThrows(UnknownHostException.class)
    private static String getHostAddress() {
        return Inet4Address.getLocalHost().getHostAddress();
    }

    private CuratorFramework startClient() {
        val client = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(RETRY_SLEEP_MS, MAX_RETRY));
        client.start();
        return client;
    }

    private ZKSegmentContainerMonitor createContainerMonitor(
            SegmentContainerRegistry registry, CuratorFramework zkClient) {
        return new ZKSegmentContainerMonitor(registry, zkClient, PRAVEGA_SERVICE_ENDPOINT, executorService());
    }

    private void initializeHostContainerMapping(CuratorFramework zkClient) throws Exception {
        HashMap<Host, Set<Integer>> mapping = new HashMap<>();
        zkClient.create().creatingParentsIfNeeded().forPath(PATH, SerializationUtils.serialize(mapping));
    }

    private SegmentContainerRegistry createMockContainerRegistry() {
        SegmentContainerRegistry containerRegistry = mock(SegmentContainerRegistry.class);
        ContainerHandle containerHandle1 = mock(ContainerHandle.class);
        when(containerHandle1.getContainerId()).thenReturn(1);
        when(containerRegistry.startContainer(anyInt(), any()))
                .thenReturn(CompletableFuture.completedFuture(containerHandle1));
        when(containerRegistry.stopContainer(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        return containerRegistry;
    }

    @SuppressWarnings("unchecked")
    private HashMap<Host, Set<Integer>> deserialize(CuratorFramework zkClient, String path) throws Exception {
        return (HashMap<Host, Set<Integer>>) SerializationUtils.deserialize(zkClient.getData().forPath(path));
    }
}
