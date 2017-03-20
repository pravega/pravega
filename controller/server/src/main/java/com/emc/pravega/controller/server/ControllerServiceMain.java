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
            CountDownLatch sessionExpiryLatch = new CountDownLatch(1);

            if (serviceConfig.getControllerClusterListenerConfig().isPresent()) {

                // Controller cluster feature is enabled.
                while (isRunning()) {
                    // Create store client.
                    log.info("Creating store client");
                    StoreClient storeClient = StoreClientFactory.createStoreClient(serviceConfig.getStoreClientConfig());

                    // Client should be ZK if controller cluster is enabled.
                    CuratorFramework client = (CuratorFramework) storeClient.getClient();

                    log.info("Awaiting ZK client connection to ZK server");
                    client.blockUntilConnected();

                    // Start controller services.
                    log.info("Starting controller services");
                    starter = new ControllerServiceStarter(serviceConfig, storeClient);
                    starter.startAsync();
                    staterInitialized.countDown();

                    // Await ZK session expiry.
                    log.info("Awaiting ZK session expiry or termination of ControllerServiceMain");
                    client.getZookeeperClient().getZooKeeper().register(new ZKWatcher(sessionExpiryLatch));

                    boolean expired = false;
                    while (isRunning() && !expired) {
                        expired = sessionExpiryLatch.await(1500, TimeUnit.MILLISECONDS);
                    }

                    // Problem of curator automatically recreating ZK client on session expiry is mitigated by
                    // employing a custom ZookeeperFactory that always returns the same ZK client to curator

                    // Once ZK session expires or once ControllerServiceMain is externally stopped,
                    // stop ControllerServiceStarter.
                    log.info("Stopping ControllerServiceStarter");
                    starter.stopAsync();

                    log.info("Awaiting termination of ControllerServiceStarter");
                    starter.awaitTerminated();
                }
            } else {

                // Create store client.
                log.info("Creating store client");
                StoreClient storeClient = StoreClientFactory.createStoreClient(serviceConfig.getStoreClientConfig());

                // Start controller services.
                log.info("Starting controller services");
                starter = new ControllerServiceStarter(serviceConfig, storeClient);
                starter.startAsync();
                staterInitialized.countDown();

                // Await termination of controller services.
                log.info("Awaiting termination of ControllerServiceStarter or termination of ControllerServiceMain");
                boolean terminated;
                do {
                    terminated = awaitStarterTerminated(1500, TimeUnit.MILLISECONDS);
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
