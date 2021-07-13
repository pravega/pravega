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
package io.pravega.common.cluster.zkImpl;

import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.pravega.test.common.TestingServerStarter;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

    private TestingServer zkTestServer;
    private String zkUrl;

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServerStarter().start();
        zkUrl = zkTestServer.getConnectString();
    }

    @After
    public void stopZookeeper() throws IOException {
        zkTestServer.close();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void registerNode() throws Exception {
        LinkedBlockingQueue<String> nodeAddedQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<String> nodeRemovedQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Exception> exceptionsQueue = new LinkedBlockingQueue<>();

        //ClusterListener for testing purposes
        @Cleanup
        CuratorFramework client2 = CuratorFrameworkFactory.builder()
                .connectString(zkUrl)
                .retryPolicy(new ExponentialBackoffRetry(
                        RETRY_SLEEP_MS, MAX_RETRY))
                .namespace(CLUSTER_NAME)
                .build();
        @Cleanup
        Cluster clusterListener = new ClusterZKImpl(client2, ClusterType.HOST);
        clusterListener.addListener((eventType, host) -> {
            switch (eventType) {
                case HOST_ADDED:
                    nodeAddedQueue.offer(host.getIpAddr());
                    break;
                case HOST_REMOVED:
                    nodeRemovedQueue.offer(host.getIpAddr());
                    break;
                case ERROR:
                    exceptionsQueue.offer(new RuntimeException("Encountered error"));
                    break;
                default:
                    exceptionsQueue.offer(new RuntimeException("Unhandled case"));
                    break;
            }
        });

        @Cleanup
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zkUrl)
                .retryPolicy(new ExponentialBackoffRetry(
                        RETRY_SLEEP_MS, MAX_RETRY))
                .namespace(CLUSTER_NAME)
                .build();

        //Create Add a node to the cluster.
        @Cleanup
        Cluster clusterZKInstance1 = new ClusterZKImpl(client, ClusterType.HOST);
        clusterZKInstance1.registerHost(new Host(HOST_1, PORT, null));
        assertEquals(HOST_1, nodeAddedQueue.poll(5, TimeUnit.SECONDS));

        //Create a separate instance of Cluster and add node to same Cluster
        @Cleanup
        Cluster clusterZKInstance2 = new ClusterZKImpl(client, ClusterType.HOST);
        clusterZKInstance1.registerHost(new Host(HOST_2, PORT, null));
        assertEquals(HOST_2, nodeAddedQueue.poll(5, TimeUnit.SECONDS));
        assertEquals(2, clusterListener.getClusterMembers().size());

        Exception exception = exceptionsQueue.poll();
        if (exception != null) {
            throw exception;
        }
    }

    @Test(timeout = TEST_TIMEOUT)
    public void deregisterNode() throws Exception {
        LinkedBlockingQueue<String> nodeAddedQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<String> nodeRemovedQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Exception> exceptionsQueue = new LinkedBlockingQueue<>();

        @Cleanup
        CuratorFramework client2 = CuratorFrameworkFactory.builder()
                .connectString(zkUrl)
                .retryPolicy(new ExponentialBackoffRetry(
                        RETRY_SLEEP_MS, MAX_RETRY))
                .namespace(CLUSTER_NAME_2)
                .build();
        @Cleanup
        Cluster clusterListener = new ClusterZKImpl(client2, ClusterType.HOST);
        clusterListener.addListener((eventType, host) -> {
            switch (eventType) {
                case HOST_ADDED:
                    nodeAddedQueue.offer(host.getIpAddr());
                    break;
                case HOST_REMOVED:
                    nodeRemovedQueue.offer(host.getIpAddr());
                    break;
                case ERROR:
                    exceptionsQueue.offer(new RuntimeException("Encountered error"));
                    break;
                default:
                    exceptionsQueue.offer(new RuntimeException("Unhandled case"));
                    break;
            }
        });

        @Cleanup
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zkUrl)
                .retryPolicy(new ExponentialBackoffRetry(
                        RETRY_SLEEP_MS, MAX_RETRY))
                .namespace(CLUSTER_NAME_2)
                .build();
        //Create Add a node to the cluster.
        @Cleanup
        Cluster clusterZKInstance1 = new ClusterZKImpl(client, ClusterType.HOST);
        clusterZKInstance1.registerHost(new Host(HOST_1, PORT, null));
        assertEquals(HOST_1, nodeAddedQueue.poll(5, TimeUnit.SECONDS));

        clusterZKInstance1.deregisterHost(new Host(HOST_1, PORT, null));
        assertEquals(HOST_1, nodeRemovedQueue.poll(5, TimeUnit.SECONDS));

        Exception exception = exceptionsQueue.poll();
        if (exception != null) {
            throw exception;
        }
    }
}
