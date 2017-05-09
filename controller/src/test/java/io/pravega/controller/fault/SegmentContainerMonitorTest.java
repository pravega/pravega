/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.fault;

import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.util.Config;
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

    private final static String CLUSTER_NAME = "testcluster";
    
    //Ensure each test completes within 30 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);
    
    private TestingServer zkTestServer;
    private CuratorFramework zkClient;
    private Cluster cluster;


    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServerStarter().start();
        String zkUrl = zkTestServer.getConnectString();

        zkClient = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();
        cluster = new ClusterZKImpl(zkClient, ClusterType.HOST);
    }

    @After
    public void stopZookeeper() throws Exception {
        cluster.close();
        zkClient.close();
        zkTestServer.close();
    }

    @Test
    public void testMonitorWithZKStore() throws Exception {
        HostMonitorConfig config = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(true)
                .containerCount(Config.HOST_STORE_CONTAINER_COUNT)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .build();
        HostControllerStore hostStore = HostStoreFactory.createStore(config,
                StoreClientFactory.createZKStoreClient(zkClient));
        testMonitor(hostStore);
    }

    @Test
    public void testMonitorWithInMemoryStore() throws Exception {
        HostMonitorConfig config = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(false)
                .containerCount(Config.HOST_STORE_CONTAINER_COUNT)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(Config.SERVICE_HOST,
                        Config.SERVICE_PORT, Config.HOST_STORE_CONTAINER_COUNT))
                .build();
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(config);
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
                new UniformContainerBalancer(), 5);
        monitor.startAsync().awaitRunning();

        assertEquals(hostStore.getContainerCount(), Config.HOST_STORE_CONTAINER_COUNT);

        //Rebalance should be triggered for the very first attempt. Verify that no hosts are added to the store.
        assertTrue(sync.tryAcquire(10, TimeUnit.SECONDS));
        assertEquals(0, hostStore.getHostContainersMap().size());

        //New host added.
        cluster.registerHost(new Host("localhost1", 1, null));
        assertTrue(sync.tryAcquire(10, TimeUnit.SECONDS));
        assertEquals(1, hostStore.getHostContainersMap().size());

        //Multiple hosts added and removed.
        cluster.registerHost(new Host("localhost2", 2, null));
        cluster.registerHost(new Host("localhost3", 3, null));
        cluster.registerHost(new Host("localhost4", 4, null));
        cluster.deregisterHost(new Host("localhost1", 1, null));
        assertTrue(sync.tryAcquire(10, TimeUnit.SECONDS));
        assertEquals(3, hostStore.getHostContainersMap().size());

        //Add a host.
        cluster.registerHost(new Host("localhost1", 1, null));

        //Rebalance should not have been triggered since the min rebalance interval is not yet elapsed.
        assertEquals(3, hostStore.getHostContainersMap().size());

        //Wait for rebalance and verify the host update.
        assertTrue(sync.tryAcquire(10, TimeUnit.SECONDS));
        assertEquals(4, hostStore.getHostContainersMap().size());

        monitor.shutDown();
    }
}
