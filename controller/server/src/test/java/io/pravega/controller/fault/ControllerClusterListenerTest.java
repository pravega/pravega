/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.fault;

import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
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
    public void setup() {
        // 1. Start ZK server.
        try {
            zkServer = new TestingServerStarter().start();
        } catch (Exception e) {
            Assert.fail("Failed starting ZK test server");
        }

        // 2. Start ZK client.
        curatorClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        curatorClient.start();

        // 3. Start executor service.
        executor = Executors.newScheduledThreadPool(5);

        // 4. start cluster event listener
        clusterZK = new ClusterZKImpl(curatorClient, ClusterType.CONTROLLER);

        try {
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
        } catch (Exception e) {
            log.error("Error adding listener to cluster", e);
            Assert.fail();
        }
    }

    @After
    public void shutdown() {
        try {
            clusterZK.close();
        } catch (Exception e) {
            log.error("Error closing cluster listener");
            Assert.fail();
        }

        executor.shutdownNow();
        try {
            executor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Failed terminating executor service", e);
            Assert.fail();
        }

        curatorClient.close();

        try {
            zkServer.close();
        } catch (IOException e) {
            log.error("Error shutting down ZK test server");
            Assert.fail();
        }
    }

    @Test(timeout = 60000L)
    public void clusterListenerTest() {
        String hostName = "localhost";
        Host host = new Host(hostName, 10, "host1");
        TaskMetadataStore taskStore = TaskStoreFactory.createInMemoryStore(executor);
        TaskSweeper taskSweeper = new TaskSweeper(taskStore, host.getHostId(), executor,
                new TestTasks(taskStore, executor, host.getHostId()));

        ControllerClusterListener clusterListener =
                new ControllerClusterListener(host, clusterZK, Optional.<ControllerEventProcessors>empty(),
                        taskSweeper, executor);
        clusterListener.startAsync();

        try {
            clusterListener.awaitRunning();
        } catch (IllegalStateException e) {
            log.error("Error starting cluster listener", e);
            Assert.fail();
        }

        validateAddedNode(host.getHostId());

        // Add a new host
        Host host1 = new Host(hostName, 20, "host2");
        clusterZK.registerHost(host1);
        validateAddedNode(host1.getHostId());

        clusterZK.deregisterHost(host1);
        validateRemovedNode(host1.getHostId());

        clusterListener.stopAsync();

        try {
            clusterListener.awaitTerminated();
        } catch (IllegalStateException e) {
            log.error("Error stopping cluster listener", e);
            Assert.fail();
        }
        validateRemovedNode(host.getHostId());
    }

    private void validateAddedNode(String host) {
        try {
            assertEquals(host, nodeAddedQueue.poll(2, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            log.error("Error validating added node {}", host, e);
            Assert.fail();
        }
    }

    private void validateRemovedNode(String host) {
        try {
            assertEquals(host, nodeRemovedQueue.poll(2, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            log.error("Error validating removed node {}", host, e);
            Assert.fail();
        }
    }
}
