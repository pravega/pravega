/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.common.cluster.zkImpl;

import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.Host;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


public class ClusterZKTest {
    private final static int TEST_TIMEOUT = 30000;

    private final static String HOST_1 = "host1";
    private final static String HOST_2 = "host2";
    private final static int PORT = 1234;
    private final static String CLUSTER_NAME = "cluster-1";
    private final static String CLUSTER_NAME_2 = "cluster-2";

    private final static int RETRY_SLEEP_MS = 100;
    private final static int MAX_RETRY = 5;

    private static TestingServer zkTestServer;
    private static String zkUrl;

    @BeforeClass
    public static void startZookeeper() throws Exception {
        zkTestServer = new TestingServer();
        zkUrl = zkTestServer.getConnectString();
    }

    @AfterClass
    public static void stopZookeeper() throws IOException {
        zkTestServer.close();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void registerNode() throws Exception {
        LinkedBlockingQueue<String> nodeAddedQueue = new LinkedBlockingQueue();
        LinkedBlockingQueue<String> nodeRemovedQueue = new LinkedBlockingQueue();

        //ClusterListener for testing purposes
        CuratorFramework client2 = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));
        Cluster clusterListener = new ClusterZKImpl(client2, CLUSTER_NAME);
        clusterListener.addListener((eventType, host) -> {
            switch (eventType) {
                case HOST_ADDED:
                    nodeAddedQueue.offer(host.getIpAddr());
                    break;
                case HOST_REMOVED:
                    nodeRemovedQueue.offer(host.getIpAddr());
                    break;
            }
        });

        CuratorFramework client = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));

        //Create Add a node to the cluster.
        Cluster clusterZKInstance1 = new ClusterZKImpl(client, CLUSTER_NAME);
        clusterZKInstance1.registerHost(new Host(HOST_1, PORT));
        assertEquals(HOST_1, nodeAddedQueue.poll(5, TimeUnit.SECONDS));

        //Create a separate instance of Cluster and add node to same Cluster
        Cluster clusterZKInstance2 = new ClusterZKImpl(client, CLUSTER_NAME);
        clusterZKInstance1.registerHost(new Host(HOST_2, PORT));
        assertEquals(HOST_2, nodeAddedQueue.poll(5, TimeUnit.SECONDS));
        assertEquals(2, clusterListener.getClusterMembers().size());

        //cleanup
        clusterListener.close();
        clusterZKInstance1.close();
        clusterZKInstance2.close();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void deregisterNode() throws Exception {
        LinkedBlockingQueue<String> nodeAddedQueue = new LinkedBlockingQueue();
        LinkedBlockingQueue<String> nodeRemovedQueue = new LinkedBlockingQueue();

        CuratorFramework client2 = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));
        Cluster clusterListener = new ClusterZKImpl(client2, CLUSTER_NAME_2);
        clusterListener.addListener((eventType, host) -> {
            switch (eventType) {
                case HOST_ADDED:
                    nodeAddedQueue.offer(host.getIpAddr());
                    break;
                case HOST_REMOVED:
                    nodeRemovedQueue.offer(host.getIpAddr());
                    break;
            }
        });

        CuratorFramework client = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));
        //Create Add a node to the cluster.
        Cluster clusterZKInstance1 = new ClusterZKImpl(client, CLUSTER_NAME_2);
        clusterZKInstance1.registerHost(new Host(HOST_1, PORT));
        assertEquals(HOST_1, nodeAddedQueue.poll(5, TimeUnit.SECONDS));

        clusterZKInstance1.deregisterHost(new Host(HOST_1, PORT));
        assertEquals(HOST_1, nodeRemovedQueue.poll(5, TimeUnit.SECONDS));

        //cleanup
        clusterListener.close();
        clusterZKInstance1.close();
    }
}
