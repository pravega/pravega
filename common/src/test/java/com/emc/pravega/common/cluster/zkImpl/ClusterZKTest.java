/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.cluster.zkImpl;

import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.Host;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.emc.pravega.testcommon.TestUtils;
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
    private final static int PORT = TestUtils.randomPort();
    private final static String CLUSTER_NAME = "cluster-1";
    private final static String CLUSTER_NAME_2 = "cluster-2";

    private final static int RETRY_SLEEP_MS = 100;
    private final static int MAX_RETRY = 5;

    private TestingServer zkTestServer;
    private String zkUrl;

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer();
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

        //ClusterListener for testing purposes
        CuratorFramework client2 = CuratorFrameworkFactory.builder()
                .connectString(zkUrl)
                .retryPolicy(new ExponentialBackoffRetry(
                        RETRY_SLEEP_MS, MAX_RETRY))
                .namespace(CLUSTER_NAME)
                .build();

        Cluster clusterListener = new ClusterZKImpl(client2);
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

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zkUrl)
                .retryPolicy(new ExponentialBackoffRetry(
                        RETRY_SLEEP_MS, MAX_RETRY))
                .namespace(CLUSTER_NAME)
                .build();

        //Create Add a node to the cluster.
        Cluster clusterZKInstance1 = new ClusterZKImpl(client);
        clusterZKInstance1.registerHost(new Host(HOST_1, PORT));
        assertEquals(HOST_1, nodeAddedQueue.poll(5, TimeUnit.SECONDS));

        //Create a separate instance of Cluster and add node to same Cluster
        Cluster clusterZKInstance2 = new ClusterZKImpl(client);
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
        LinkedBlockingQueue<String> nodeAddedQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<String> nodeRemovedQueue = new LinkedBlockingQueue<>();

        CuratorFramework client2 = CuratorFrameworkFactory.builder()
                .connectString(zkUrl)
                .retryPolicy(new ExponentialBackoffRetry(
                        RETRY_SLEEP_MS, MAX_RETRY))
                .namespace(CLUSTER_NAME_2)
                .build();
        Cluster clusterListener = new ClusterZKImpl(client2);
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

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zkUrl)
                .retryPolicy(new ExponentialBackoffRetry(
                        RETRY_SLEEP_MS, MAX_RETRY))
                .namespace(CLUSTER_NAME_2)
                .build();
        //Create Add a node to the cluster.
        Cluster clusterZKInstance1 = new ClusterZKImpl(client);
        clusterZKInstance1.registerHost(new Host(HOST_1, PORT));
        assertEquals(HOST_1, nodeAddedQueue.poll(5, TimeUnit.SECONDS));

        clusterZKInstance1.deregisterHost(new Host(HOST_1, PORT));
        assertEquals(HOST_1, nodeRemovedQueue.poll(5, TimeUnit.SECONDS));

        //cleanup
        clusterListener.close();
        clusterZKInstance1.close();
    }
}
