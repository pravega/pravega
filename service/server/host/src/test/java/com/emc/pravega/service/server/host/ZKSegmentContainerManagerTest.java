/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.service.server.host;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.segment.SegmentToContainerMapper;
import com.emc.pravega.service.server.ContainerHandle;
import com.emc.pravega.service.server.SegmentContainerRegistry;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class ZKSegmentContainerManagerTest {

    private final static int TEST_TIMEOUT = 30000;
    private final static int RETRY_SLEEP_MS = 100;
    private final static int MAX_RETRY = 5;
    private final static int PORT = 12345;
    private final static Host PRAVEGA_SERVICE_ENDPOINT = new Host(getHostAddress(), PORT);
    private final static String CLUSTER_NAME = "cluster-1";
    private final static String PATH = ZKPaths.makePath("cluster", CLUSTER_NAME, "segmentContainerHostMapping");
    private static String zkUrl;

    private static TestingServer zkTestServer;

    @Mock
    private SegmentToContainerMapper segmentToContainerMapper;

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer();
        zkUrl = zkTestServer.getConnectString();
    }

    @After
    public void stopZookeeper() throws IOException {
        zkTestServer.close();
    }

    //Test if no mapping is present is present in zk.
    @Test(timeout = TEST_TIMEOUT)
    public void initializeErrorTest() throws Exception {

        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));
        zkClient.start();

        segmentToContainerMapper = new SegmentToContainerMapper(8);

        ZKSegmentContainerManager segManager = new ZKSegmentContainerManager(createMockContainerRegistry(),
                segmentToContainerMapper, zkClient,
                PRAVEGA_SERVICE_ENDPOINT, CLUSTER_NAME);

        CompletableFuture<Void> result = segManager.initialize();

        assertEquals(false, result.isCompletedExceptionally());

        zkClient.close();
    }


    @Test(timeout = TEST_TIMEOUT)
    public void listenerTest() throws Exception {
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));
        zkClient.start();

        SegmentContainerRegistry containerRegistry = createMockContainerRegistry();

        segmentToContainerMapper = new SegmentToContainerMapper(8);

        ZKSegmentContainerManager segManager = new ZKSegmentContainerManager(containerRegistry,
                segmentToContainerMapper, zkClient,
                PRAVEGA_SERVICE_ENDPOINT, CLUSTER_NAME);

        CompletableFuture<Void> result = segManager.initialize();
        initializeSegmentMapping(zkClient);

        ContainerHandle containerHandle2 = mock(ContainerHandle.class);
        when(containerHandle2.getContainerId()).thenReturn(2);
        when(containerRegistry.startContainer(eq(2), any()))
                .thenReturn(CompletableFuture.completedFuture(containerHandle2));

        //now modify the ZK entry
        HashMap<Host, Set<Integer>> currentData =
                (HashMap<Host, Set<Integer>>) SerializationUtils.deserialize(zkClient.getData().forPath(PATH));
        currentData.put(PRAVEGA_SERVICE_ENDPOINT, new HashSet(Arrays.asList(1, 2)));
        zkClient.setData().forPath(PATH, SerializationUtils.serialize(currentData));

        verify(containerHandle2, after(500).atMost(5)).getContainerId();
        assertEquals(2, segManager.getHandles().size());
        assertTrue(segManager.getHandles().containsKey(1));
        assertTrue(segManager.getHandles().containsKey(2));

        zkClient.close();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void closeMethodTest() throws Exception {
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));
        zkClient.start();

        SegmentContainerRegistry containerRegistry = mock(SegmentContainerRegistry.class);
        ContainerHandle containerHandle1 = mock(ContainerHandle.class);
        when(containerHandle1.getContainerId()).thenReturn(1);
        when(containerRegistry.startContainer(eq(1), any()))
                .thenReturn(CompletableFuture.completedFuture(containerHandle1));
        when(containerRegistry.stopContainer(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        segmentToContainerMapper = new SegmentToContainerMapper(8);

        ZKSegmentContainerManager segManager = new ZKSegmentContainerManager(containerRegistry,
                segmentToContainerMapper, zkClient,
                PRAVEGA_SERVICE_ENDPOINT, CLUSTER_NAME);

        CompletableFuture<Void> result = segManager.initialize();

        segManager.close();
        assertEquals(0, segManager.getHandles().size());
    }

    private static String getHostAddress() {
        try {
            return Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unable to get the Host Address", e);
        }
    }

    private void initializeSegmentMapping(CuratorFramework zkClient) throws Exception {
        HashMap<Host, Set<Integer>> mapping = new HashMap<>();
        mapping.put(PRAVEGA_SERVICE_ENDPOINT, new HashSet<>(Arrays.asList(1)));
        zkClient.create().creatingParentsIfNeeded().forPath(PATH, SerializationUtils.serialize(mapping));
    }

    private SegmentContainerRegistry createMockContainerRegistry() {
        SegmentContainerRegistry containerRegistry = mock(SegmentContainerRegistry.class);
        ContainerHandle containerHandle1 = mock(ContainerHandle.class);
        when(containerHandle1.getContainerId()).thenReturn(1);
        when(containerRegistry.startContainer(anyInt(), any()))
                .thenReturn(CompletableFuture.completedFuture(containerHandle1));
        return containerRegistry;
    }
}
