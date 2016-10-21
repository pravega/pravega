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
package com.emc.pravega.service.server;

import com.emc.pravega.common.cluster.Host;
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
import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ZKSegmentContainerManagerTest {

    private static final String ZK_URL = "localhost:2182";
    private final static int RETRY_SLEEP_MS = 100;
    private final static int MAX_RETRY = 5;
    private static final int PORT = 12345;

    private static TestingServer zkTestServer;
    private static String path = ZKPaths.makePath("cluster", "segmentContainerHostMapping");

    @Mock
    private SegmentToContainerMapper segmentToContainerMapper;

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer(2182);
    }

    @After
    public void stopZookeeper() throws IOException {

        zkTestServer.close();
    }

    @Test
    public void initializeTest() throws Exception {
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(ZK_URL, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));
        zkClient.start();

        initializeSegmentMapping(zkClient);

        SegmentContainerRegistry containerRegistry = mock(SegmentContainerRegistry.class);
        ContainerHandle containerHandle1 = mock(ContainerHandle.class);
        when(containerHandle1.getContainerId()).thenReturn(1);
        when(containerRegistry.startContainer(anyInt(), any())).thenReturn(CompletableFuture.completedFuture(containerHandle1));
        segmentToContainerMapper = new SegmentToContainerMapper(8);

        ZKSegmentContainerManager segManager = new ZKSegmentContainerManager(containerRegistry, segmentToContainerMapper, zkClient);

        CompletableFuture<Void> result = segManager.initialize(Duration.ofSeconds(5));

        assertEquals(1, segManager.getHandles().size());
        assertEquals(1, segManager.getHandles().get(1).getContainerId());
        assertEquals(false, result.isCompletedExceptionally());

        zkClient.close();
    }

    @Test
    public void listenerTest() throws Exception {
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(ZK_URL, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));
        zkClient.start();

        initializeSegmentMapping(zkClient);

        SegmentContainerRegistry containerRegistry = mock(SegmentContainerRegistry.class);
        ContainerHandle containerHandle1 = mock(ContainerHandle.class);
        when(containerHandle1.getContainerId()).thenReturn(1);
        when(containerRegistry.startContainer(eq(1), any())).thenReturn(CompletableFuture.completedFuture(containerHandle1));
        segmentToContainerMapper = new SegmentToContainerMapper(8);

        ZKSegmentContainerManager segManager = new ZKSegmentContainerManager(containerRegistry, segmentToContainerMapper, zkClient);

        CompletableFuture<Void> result = segManager.initialize(Duration.ofSeconds(5));

        ContainerHandle containerHandle2 = mock(ContainerHandle.class);
        when(containerHandle2.getContainerId()).thenReturn(2);
        when(containerRegistry.startContainer(eq(2), any())).thenReturn(CompletableFuture.completedFuture(containerHandle2));

        //now modify the ZK entry
        HashMap<Integer, Host> currentData = (HashMap<Integer, Host>) SerializationUtils.deserialize(zkClient.getData().forPath(path));
        currentData.put(2, new Host(getHostAddress(), PORT));
        zkClient.setData().forPath(path, SerializationUtils.serialize(currentData));

        verify(containerHandle2, after(500).atMost(5)).getContainerId();
        assertEquals(2, segManager.getHandles().size());
        assertEquals(1, segManager.getHandles().get(1).getContainerId());
        assertEquals(2, segManager.getHandles().get(2).getContainerId());

        zkClient.close();
    }

    @Test
    public void closeMethodTest() throws Exception {
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(ZK_URL, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));
        zkClient.start();

        initializeSegmentMapping(zkClient);

        SegmentContainerRegistry containerRegistry = mock(SegmentContainerRegistry.class);
        ContainerHandle containerHandle1 = mock(ContainerHandle.class);
        when(containerHandle1.getContainerId()).thenReturn(1);
        when(containerRegistry.startContainer(eq(1), any())).thenReturn(CompletableFuture.completedFuture(containerHandle1));
        when(containerRegistry.stopContainer(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        segmentToContainerMapper = new SegmentToContainerMapper(8);

        ZKSegmentContainerManager segManager = new ZKSegmentContainerManager(containerRegistry, segmentToContainerMapper, zkClient);

        CompletableFuture<Void> result = segManager.initialize(Duration.ofSeconds(5));

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
        HashMap<Integer, Host> mapping = new HashMap<>();
        mapping.put(1, new Host(getHostAddress(), PORT));
        zkClient.create().creatingParentsIfNeeded().forPath(path, SerializationUtils.serialize(mapping));
    }
}