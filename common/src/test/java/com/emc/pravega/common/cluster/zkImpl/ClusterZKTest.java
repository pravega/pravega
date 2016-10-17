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
package com.emc.pravega.common.cluster.zkImpl;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.NodeType;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


public class ClusterZKTest {

    private static final String ZK_URL = "localhost:2182";
    private final static String HOST_1 = "host1";
    private final static String HOST_2 = "host2";
    private final static int PORT = 1234;
    private final static String CLUSTER_NAME = "cluster-1";
    private final static String CLUSTER_NAME_2 = "cluster-2";

    private final static int RETRY_SLEEP_MS = 100;
    private final static int MAX_RETRY = 5;

    private static TestingServer zkTestServer;

    @BeforeClass
    public static void startZookeeper() throws Exception {
        zkTestServer = new TestingServer(2182);
    }

    @AfterClass
    public static void stopZookeeper() throws IOException {
        zkTestServer.close();
    }

    @Test
    public void registerNode() throws Exception {

        //TestClusterListener for testing purposes
        CuratorFramework client2 = CuratorFrameworkFactory.newClient(ZK_URL, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));
        TestClusterListener clusterListener = new TestClusterListener(client2, CLUSTER_NAME, NodeType.DATA);
        clusterListener.start();

        CuratorFramework client = CuratorFrameworkFactory.newClient(ZK_URL, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));

        //Create Add a node to the cluster.
        ClusterZKImpl clusterZKInstance1 = new ClusterZKImpl(client, CLUSTER_NAME, NodeType.DATA);
        clusterZKInstance1.registerNode(new Host(HOST_1, PORT));
        assertEquals(HOST_1, clusterListener.nodeAddedQueue.poll(5, TimeUnit.SECONDS));

        //Create a separate instance of Cluster and add node to same Cluster
        ClusterZKImpl clusterZKInstance2 = new ClusterZKImpl(client, CLUSTER_NAME, NodeType.DATA);
        clusterZKInstance1.registerNode(new Host(HOST_2, PORT));
        assertEquals(HOST_2, clusterListener.nodeAddedQueue.poll(5, TimeUnit.SECONDS));
        assertEquals(2, clusterListener.getClusterMembers().size());

        //cleanup
        clusterListener.close();
        clusterZKInstance1.close();
        clusterZKInstance2.close();
    }

    @Test
    public void deregisterNode() throws Exception {
        CuratorFramework client2 = CuratorFrameworkFactory.newClient(ZK_URL, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));
        TestClusterListener clusterListener = new TestClusterListener(client2, CLUSTER_NAME_2, NodeType.DATA);
        clusterListener.start();

        CuratorFramework client = CuratorFrameworkFactory.newClient(ZK_URL, new ExponentialBackoffRetry(
                RETRY_SLEEP_MS, MAX_RETRY));
        //Create Add a node to the cluster.
        ClusterZKImpl clusterZKInstance1 = new ClusterZKImpl(client, CLUSTER_NAME_2, NodeType.DATA);
        clusterZKInstance1.registerNode(new Host(HOST_1, PORT));
        assertEquals(HOST_1, clusterListener.nodeAddedQueue.poll(5, TimeUnit.SECONDS));

        clusterZKInstance1.deregisterNode(new Host(HOST_1, PORT));
        assertEquals(HOST_1, clusterListener.nodeRemovedQueue.poll(5, TimeUnit.SECONDS));

        //cleanup
        clusterListener.close();
        clusterZKInstance1.close();
    }
}
