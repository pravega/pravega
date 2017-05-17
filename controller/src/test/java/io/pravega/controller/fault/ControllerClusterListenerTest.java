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

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.task.Stream.TxnSweeper;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.TestTasks;
import io.pravega.controller.task.TaskSweeper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * ControllerClusterListener tests.
 */
@Slf4j
public class ControllerClusterListenerTest {

    private TestingServer zkServer;
    private CuratorFramework curatorClient;
    private ScheduledExecutorService executor;
    private ClusterZKImpl clusterZK;

    private LinkedBlockingQueue<String> nodeAddedQueue = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<String> nodeRemovedQueue = new LinkedBlockingQueue<>();


    @Before
    public void setup() throws Exception {
        // 1. Start ZK server.
        zkServer = new TestingServerStarter().start();

        // 2. Start ZK client.
        curatorClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        curatorClient.start();

        // 3. Start executor service.
        executor = Executors.newScheduledThreadPool(5);

        // 4. start cluster event listener
        clusterZK = new ClusterZKImpl(curatorClient, ClusterType.CONTROLLER);

        clusterZK.addListener((eventType, host) -> {
            switch (eventType) {
                case HOST_ADDED:
                    nodeAddedQueue.offer(host.getHostId());
                    break;
                case HOST_REMOVED:
                    nodeRemovedQueue.offer(host.getHostId());
                    break;
                case ERROR:
                default:
                    break;
            }
        });
    }

    @After
    public void shutdown() throws Exception {
        clusterZK.close();
        executor.shutdownNow();
        executor.awaitTermination(2, TimeUnit.SECONDS);
        curatorClient.close();
        zkServer.close();
    }

    @Test(timeout = 60000L)
    public void clusterListenerTest() throws InterruptedException {
        String hostName = "localhost";
        Host host = new Host(hostName, 10, "host1");

        // Create task sweeper.
        TaskMetadataStore taskStore = TaskStoreFactory.createInMemoryStore(executor);
        TaskSweeper taskSweeper = new TaskSweeper(taskStore, host.getHostId(), executor,
                new TestTasks(taskStore, executor, host.getHostId()));

        // Create txn sweeper.
        StreamMetadataStore streamStore = StreamStoreFactory.createInMemoryStore(executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        ConnectionFactory connectionFactory = Mockito.mock(ConnectionFactory.class);
        StreamTransactionMetadataTasks txnTasks = new StreamTransactionMetadataTasks(streamStore, hostStore,
                segmentHelper, executor, host.getHostId(), connectionFactory);
        txnTasks.initializeStreamWriters("commitStream", new EventStreamWriterMock<>(), "abortStream",
                new EventStreamWriterMock<>());
        TxnSweeper txnSweeper = new TxnSweeper(streamStore, txnTasks, 100, executor);

        // Create ControllerClusterListener.
        ControllerClusterListener clusterListener =
                new ControllerClusterListener(host, clusterZK, Optional.empty(),
                        taskSweeper, Optional.of(txnSweeper), executor);
        clusterListener.startAsync();

        clusterListener.awaitRunning();

        validateAddedNode(host.getHostId());

        // Add a new host
        Host host1 = new Host(hostName, 20, "host2");
        clusterZK.registerHost(host1);
        validateAddedNode(host1.getHostId());

        clusterZK.deregisterHost(host1);
        validateRemovedNode(host1.getHostId());

        clusterListener.stopAsync();

        clusterListener.awaitTerminated();
        validateRemovedNode(host.getHostId());
    }

    private void validateAddedNode(String host) throws InterruptedException {
        assertEquals(host, nodeAddedQueue.poll(2, TimeUnit.SECONDS));
    }

    private void validateRemovedNode(String host) throws InterruptedException {
        assertEquals(host, nodeRemovedQueue.poll(2, TimeUnit.SECONDS));
    }
}
