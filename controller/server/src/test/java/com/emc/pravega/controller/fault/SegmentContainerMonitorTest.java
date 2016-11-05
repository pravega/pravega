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
import com.emc.pravega.controller.store.host.ZKConfig;
import com.emc.pravega.controller.util.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.assertEquals;

@Slf4j
public class SegmentContainerMonitorTest {

    private static TestingServer zkTestServer;
    private static CuratorFramework zkClient;
    private static Cluster cluster;

    private final static String CLUSTER_NAME = "testcluster";

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer();
        String zkUrl = zkTestServer.getConnectString();

        zkClient = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(200, 10, 5000));
        cluster = new ClusterZKImpl(zkClient, CLUSTER_NAME);
    }

    @After
    public void stopZookeeper() throws Exception {
        cluster.close();
        zkTestServer.close();
    }

    //Disabling the test for regular builds since this is time sensitive and depends on minRebalanceInterval to be less
    //than 1 second.
    //TODO: enable this test by default.
    //@Test
    public void testMonitorWithZKStore() throws Exception {

        cluster.registerHost(new Host("localhost1", 1));

        HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.Zookeeper,
                new ZKConfig(zkClient, CLUSTER_NAME));

        SegmentContainerMonitor monitor = new SegmentContainerMonitor(hostStore, zkClient, CLUSTER_NAME);
        monitor.start();

        assertEquals(hostStore.getContainerCount(), Config.HOST_STORE_CONTAINER_COUNT);

        Thread.sleep(1000);
        assertEquals(1, hostStore.getHostContainersMap().size());

        cluster.registerHost(new Host("localhost2", 2));
        cluster.registerHost(new Host("localhost3", 3));
        cluster.registerHost(new Host("localhost4", 4));
        cluster.deregisterHost(new Host("localhost1", 1));

        Thread.sleep(1000);
        assertEquals(3, hostStore.getHostContainersMap().size());

        cluster.registerHost(new Host("localhost1", 1));

        Thread.sleep(1000);
        assertEquals(4, hostStore.getHostContainersMap().size());

        monitor.close();
    }

    //Disabling the test for regular builds since this is time sensitive and depends on minRebalanceInterval to be less
    //than 1 second.
    //TODO: enable this test by default.
    //@Test
    public void testMonitorWithInMemoryStore() throws Exception {

        cluster.registerHost(new Host("localhost1", 1));

        HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory, null);

        SegmentContainerMonitor monitor = new SegmentContainerMonitor(hostStore, zkClient, CLUSTER_NAME);
        monitor.start();

        assertEquals(hostStore.getContainerCount(), Config.HOST_STORE_CONTAINER_COUNT);

        Thread.sleep(1000);
        assertEquals(1, hostStore.getHostContainersMap().size());

        cluster.registerHost(new Host("localhost2", 2));
        cluster.registerHost(new Host("localhost3", 3));
        cluster.registerHost(new Host("localhost4", 4));
        cluster.deregisterHost(new Host("localhost1", 1));

        Thread.sleep(1000);
        assertEquals(3, hostStore.getHostContainersMap().size());

        cluster.registerHost(new Host("localhost1", 1));

        Thread.sleep(1000);
        assertEquals(4, hostStore.getHostContainersMap().size());

        monitor.close();
    }

}
