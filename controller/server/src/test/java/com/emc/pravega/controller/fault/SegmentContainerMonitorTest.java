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
package com.emc.pravega.controller.fault;

import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.zkImpl.ClusterZKImpl;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.host.ZKHostStore;
import com.emc.pravega.controller.util.Config;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SegmentContainerMonitorTest {

    private static TestingServer zkTestServer;
    private static CuratorFramework zkClient;
    private static Cluster cluster;

    private final static String CLUSTER_NAME = "testcluster";

    //Ensure each test completes within 30 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer();
        String zkUrl = zkTestServer.getConnectString();

        zkClient = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();
        cluster = new ClusterZKImpl(zkClient, CLUSTER_NAME);
    }

    @After
    public void stopZookeeper() throws Exception {
        cluster.close();
        zkTestServer.close();
    }

    @Test
    public void testMonitorWithZKStore() throws Exception {
        HostControllerStore hostStore = new ZKHostStore(zkClient, CLUSTER_NAME);
        testMonitor(hostStore);
    }

    @Test
    public void testMonitorWithInMemoryStore() throws Exception {
        HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);
        testMonitor(hostStore);
    }

    private void testMonitor(HostControllerStore hostStore) throws Exception {
        //To coordinate the test cases.
        Semaphore sync = new Semaphore(0);

        //Decorating hostStore to add the coordination logic using semaphore.
        class MockHostControllerStore implements HostControllerStore {

            @Override
            public Map<Host, Set<Integer>> getHostContainersMap() {
                return hostStore.getHostContainersMap();
            }

            @Override
            public void updateHostContainersMap(Map<Host, Set<Integer>> newMapping) {
                hostStore.updateHostContainersMap(newMapping);
                //Notify the test case of the update.
                sync.release();
            }
            
            @Override
            public int getContainerCount() {
                return hostStore.getContainerCount();
            }
            
            @Override
            public Host getHostForSegment(String scope, String stream, int segmentNumber) {
                return null;
            }
        }

        SegmentContainerMonitor monitor = new SegmentContainerMonitor(new MockHostControllerStore(), zkClient,
                CLUSTER_NAME, new UniformContainerBalancer(), 5);
        monitor.startAsync().awaitRunning();

        assertEquals(hostStore.getContainerCount(), Config.HOST_STORE_CONTAINER_COUNT);

        //Rebalance should be triggered for the very first attempt. Verify that no hosts are added to the store.
        assertTrue(sync.tryAcquire(10, TimeUnit.SECONDS));
        assertEquals(0, hostStore.getHostContainersMap().size());

        //New host added.
        cluster.registerHost(new Host("localhost1", 1));
        assertTrue(sync.tryAcquire(10, TimeUnit.SECONDS));
        assertEquals(1, hostStore.getHostContainersMap().size());

        //Multiple hosts added and removed.
        cluster.registerHost(new Host("localhost2", 2));
        cluster.registerHost(new Host("localhost3", 3));
        cluster.registerHost(new Host("localhost4", 4));
        cluster.deregisterHost(new Host("localhost1", 1));
        assertTrue(sync.tryAcquire(10, TimeUnit.SECONDS));
        assertEquals(3, hostStore.getHostContainersMap().size());

        //Add a host.
        cluster.registerHost(new Host("localhost1", 1));

        //Rebalance should not have been triggered since the min rebalance interval is not yet elapsed.
        assertEquals(3, hostStore.getHostContainersMap().size());

        //Wait for rebalance and verify the host update.
        assertTrue(sync.tryAcquire(10, TimeUnit.SECONDS));
        assertEquals(4, hostStore.getHostContainersMap().size());

        monitor.shutDown();
    }
}
