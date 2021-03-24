/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.host;

import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.HostContainerMap;
import io.pravega.segmentstore.server.ContainerHandle;
import io.pravega.segmentstore.server.SegmentContainerRegistry;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ZKSegmentContainerManagerTest extends ThreadPooledTestSuite {

    private final static int TEST_TIMEOUT = 60000;
    private final static int RETRY_SLEEP_MS = 100;
    private final static int MAX_RETRY = 5;
    private final static int MAX_PARALLEL_CONTAINER_STARTS = 2;
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
     * Test if initialization completes.
     *
     * @throws Exception if an error occurred.
     */
    @Test
    public void testInitializeSucceeds() throws Exception {
        @Cleanup
        CuratorFramework zkClient = startClient();
        @Cleanup
        ZKSegmentContainerManager segManager = createContainerManager(createMockContainerRegistry(), zkClient);
        segManager.initialize();
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
        segManager.initialize();
        segManager.close();
    }

    @Test
    public void testContainerStart() throws Exception {
        @Cleanup
        CuratorFramework zkClient = startClient();
        initializeHostContainerMapping(zkClient);

        SegmentContainerRegistry containerRegistry = mock(SegmentContainerRegistry.class);
        ContainerHandle containerHandle1 = mock(ContainerHandle.class);
        when(containerHandle1.getContainerId()).thenReturn(1);
        when(containerRegistry.startContainer(eq(1), any()))
                .thenReturn(CompletableFuture.completedFuture(containerHandle1));

        @Cleanup
        ZKSegmentContainerManager segManager = createContainerManager(containerRegistry, zkClient);
        segManager.initialize();
        verify(containerRegistry, timeout(30000).atLeastOnce()).startContainer(eq(1), any());
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
        return new ZKSegmentContainerManager(registry, zkClient, PRAVEGA_SERVICE_ENDPOINT, MAX_PARALLEL_CONTAINER_STARTS, executorService());
    }

    private void initializeHostContainerMapping(CuratorFramework zkClient) throws Exception {
        HashMap<Host, Set<Integer>> mapping = new HashMap<>();
        mapping.put(PRAVEGA_SERVICE_ENDPOINT, Collections.singleton(1));
        zkClient.create().creatingParentsIfNeeded().forPath(PATH, HostContainerMap.createHostContainerMap(mapping).toBytes());
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
}
