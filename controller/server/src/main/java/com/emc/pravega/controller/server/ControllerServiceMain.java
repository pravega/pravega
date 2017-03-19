/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.controller.server.eventProcessor.LocalController;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.client.StoreClientFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * ControllerServiceMonitor, entry point into the controller service.
 */
@Slf4j
public class ControllerServiceMain extends AbstractExecutionThreadService {

    private final String objectId;
    private final ControllerServiceConfig serviceConfig;
    private ControllerServiceStarter starter;
    private final CountDownLatch staterInitialized;

    @AllArgsConstructor
    static class ZKWatcher implements Watcher {
        private final CountDownLatch latch;

        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == Event.KeeperState.Expired) {
                latch.countDown();
            }
        }
    }

    public ControllerServiceMain(ControllerServiceConfig serviceConfig) {
        this.objectId = "ControllerServiceMain";
        this.serviceConfig = serviceConfig;
        this.staterInitialized = new CountDownLatch(1);
    }

    @Override
    protected void run() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, this.objectId, "run");
        try {
            CountDownLatch sessionExpityLatch = new CountDownLatch(1);

            // Create store client.
            log.info("Creating store client");
            StoreClient storeClient = StoreClientFactory.createStoreClient(serviceConfig.getStoreClientConfig());

            if (serviceConfig.getControllerClusterListenerConfig().isPresent()) {
                do {
                    // If controller cluster listener is enabled,
                    CuratorFramework client = (CuratorFramework) storeClient.getClient();

                    log.info("Awaiting ZK client connection to ZK server");
                    client.blockUntilConnected();

                    // Start controller services.
                    log.info("Starting controller services");
                    starter = new ControllerServiceStarter(serviceConfig, storeClient);
                    starter.startAsync();
                    staterInitialized.countDown();

                    // Await ZK session expiry.
                    log.info("Awaiting ZK session expiry");
                    client.getZookeeperClient().getZooKeeper().register(new ZKWatcher(sessionExpityLatch));

                    boolean expired;
                    do {
                        expired = sessionExpityLatch.await(2000, TimeUnit.MILLISECONDS);
                    } while (isRunning() && !expired);

                    // TODO: When ZK session expires, curator would have created a new session and controller
                    // service components would be communicating with ZK. This problem can be mitigated by one of
                    // the following ways.
                    // 1. Change controller store implementations to use Zookeeper client instead of Curator client, so
                    //    that only leader elector uses curator framework, most of other controller code uses ZK client.
                    // 2. Configure curator to not recreate Zookeeper client automatically under the hood
                    // Can we pass a custom ZookeeperClientFactory while creating CuratorFramework for achieving 2?

                    // Once ZK session expires or once ControllerServiceMain is externally stopped,
                    // stop ControllerServiceStarter.
                    log.info("Stopping ControllerServiceStarter");
                    starter.stopAsync();

                    log.info("Awaiting termination of ControllerServiceStarter");
                    starter.awaitTerminated();
                } while (isRunning());
            } else {
                // Start controller services.
                log.info("Starting controller services");
                starter = new ControllerServiceStarter(serviceConfig, storeClient);
                starter.startAsync();
                staterInitialized.countDown();

                // Await termination of controller services.
                log.info("Awaiting termination of ControllerServiceStarter or stop of ControllerServiceMain");
                boolean terminated;
                do {
                    terminated = awaitStarterTerminated(2000, TimeUnit.MILLISECONDS);
                } while (isRunning() && !terminated);

                if (!isRunning()) {
                    // Initiate termination of controller services.
                    log.info("Stopping controller services");
                    starter.stopAsync();

                    log.info("Awaiting termination of controller services");
                    starter.awaitTerminated();
                }
            }
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "run", traceId);
        }
    }

    @VisibleForTesting
    public boolean awaitTasksModuleInitialization(long timeout, TimeUnit timeUnit) throws InterruptedException {
        staterInitialized.await();
        return this.starter.awaitTasksModuleInitialization(timeout, timeUnit);
    }

    @VisibleForTesting
    public ControllerService getControllerService() throws InterruptedException {
        staterInitialized.await();
        return this.starter.getControllerService();
    }

    @VisibleForTesting
    public LocalController getController() throws InterruptedException {
        staterInitialized.await();
        return this.starter.getController();
    }

    @VisibleForTesting
    public void awaitStarterRunning() throws InterruptedException {
        staterInitialized.await();
        this.starter.awaitRunning();
    }

    private boolean awaitStarterTerminated(long time, TimeUnit timeUnit) {
        try {
            starter.awaitTerminated(time, timeUnit);
            return true;
        } catch (TimeoutException te) {
            // ignore timeout exception
            return false;
        }
    }
}
