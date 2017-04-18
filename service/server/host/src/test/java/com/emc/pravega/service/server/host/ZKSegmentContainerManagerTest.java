/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.server.ContainerHandle;
import com.emc.pravega.service.server.SegmentContainerRegistry;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.TestUtils;
import com.emc.pravega.testcommon.TestingServerStarter;
import com.emc.pravega.testcommon.ThreadPooledTestSuite;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ZKSegmentContainerManagerTest extends ThreadPooledTestSuite {

    private final static int TEST_TIMEOUT = 30000;
    private final static int RETRY_SLEEP_MS = 100;
    private final static int MAX_RETRY = 5;
    private static final int PORT = TestUtils.getAvailableListenPort();
    private final static Host PRAVEGA_SERVICE_ENDPOINT = new Host(getHostAddress(), PORT, null);
    private final static String PATH = ZKPaths.makePath("cluster", "segmentContainerHostMapping");
    private String zkUrl;

    private TestingServer zkTestServer;

    @Rule
    public Timeout globalTimeout = Timeout.millis(TEST_TIMEOUT); // timeout per method tested

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
        ZKSegmentContainerManager segManager = createContainerManager(createMockContainerRegistry(), zkClient);
        segManager.initialize().get();
        assertEquals("Unexpected number of handles.", 0, segManager.getRegisteredContainers().size());
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
        ZKSegmentContainerManager segManager = createContainerManager(createMockContainerRegistry(), zkClient);
        zkClient.close();

        AssertExtensions.assertThrows(
                "initialize() did not throw an exception when ZooKeeper could not be accessed.",
                segManager::initialize,
                ex -> true); // Any exception will do, as long as it is propagated.
    }

    @Test
    public void testListener() throws Exception {
        @Cleanup
        CuratorFramework zkClient = startClient();
        initializeHostContainerMapping(zkClient);

        SegmentContainerRegistry containerRegistry = createMockContainerRegistry();
        @Cleanup
        ZKSegmentContainerManager segManager = createContainerManager(containerRegistry, zkClient);
        segManager.initialize().get();

        // Simulate a container that takes a while to start.
        CompletableFuture<ContainerHandle> startupFuture = new CompletableFuture<>();
        ContainerHandle containerHandle = mock(ContainerHandle.class);
        when(containerHandle.getContainerId()).thenReturn(2);
        when(containerRegistry.startContainer(eq(2), any()))
                .thenReturn(startupFuture);

        //now modify the ZK entry
        HashMap<Host, Set<Integer>> currentData = deserialize(zkClient, PATH);
        currentData.put(PRAVEGA_SERVICE_ENDPOINT, Collections.singleton(2));
        zkClient.setData().forPath(PATH, SerializationUtils.serialize(currentData));

        // Container finished starting.
        startupFuture.complete(containerHandle);
        verify(containerRegistry, timeout(10000).atLeastOnce()).startContainer(eq(2), any());
        assertTrue(segManager.getRegisteredContainers().contains(2));
    }

    @Test
    public void testShutdownNotYetStartedContainer() throws Exception {
        @Cleanup
        CuratorFramework zkClient = startClient();
        initializeHostContainerMapping(zkClient);

        SegmentContainerRegistry containerRegistry = createMockContainerRegistry();
        @Cleanup
        ZKSegmentContainerManager segManager = createContainerManager(containerRegistry, zkClient);
        segManager.initialize().get();

        // Simulate a container that takes a long time to start.
        CompletableFuture<ContainerHandle> startupFuture = new CompletableFuture<>();
        ContainerHandle containerHandle = mock(ContainerHandle.class);
        when(containerHandle.getContainerId()).thenReturn(2);
        when(containerRegistry.startContainer(eq(2), any()))
                .thenReturn(startupFuture);

        // Use ZK to send that information to the Container Manager.
        HashMap<Host, Set<Integer>> currentData = deserialize(zkClient, PATH);
        currentData.put(PRAVEGA_SERVICE_ENDPOINT, Collections.singleton(2));
        zkClient.setData().forPath(PATH, SerializationUtils.serialize(currentData));

        // Verify it's not yet started.
        verify(containerRegistry, timeout(60000).atLeastOnce()).startContainer(eq(2), any());
        assertEquals(0, segManager.getRegisteredContainers().size());

        // Now simulate shutting it down.
        when(containerRegistry.stopContainer(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        currentData.clear();
        zkClient.setData().forPath(PATH, SerializationUtils.serialize(currentData));

        startupFuture.complete(containerHandle);
        verify(containerRegistry, timeout(60000).atLeastOnce()).stopContainer(any(), any());

        assertEquals(0, segManager.getRegisteredContainers().size());
    }

    @Test
    public void testClose() throws Exception {
        @Cleanup
        CuratorFramework zkClient = startClient();

        SegmentContainerRegistry containerRegistry = mock(SegmentContainerRegistry.class);
        ContainerHandle containerHandle1 = mock(ContainerHandle.class);
        when(containerHandle1.getContainerId()).thenReturn(1);
        when(containerRegistry.startContainer(eq(1), any()))
                .thenReturn(CompletableFuture.completedFuture(containerHandle1));
        when(containerRegistry.stopContainer(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        ZKSegmentContainerManager segManager = createContainerManager(containerRegistry, zkClient);

        FutureHelpers.await(segManager.initialize());

        segManager.close();
        assertEquals(0, segManager.getRegisteredContainers().size());
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

    private ZKSegmentContainerManager createContainerManager(SegmentContainerRegistry registry, CuratorFramework zkClient) {
        return new ZKSegmentContainerManager(registry, zkClient, PRAVEGA_SERVICE_ENDPOINT, executorService());
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
