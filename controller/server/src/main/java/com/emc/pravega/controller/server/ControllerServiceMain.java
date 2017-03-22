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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * ControllerServiceMonitor, entry point into the controller service.
 */
@Slf4j
public class ControllerServiceMain extends AbstractExecutionThreadService {

    private final String objectId;
    private final ControllerServiceConfig serviceConfig;
    private ControllerServiceStarter starter;
    private final CountDownLatch staterInitialized;
    private final CompletableFuture<Void> serviceStopFuture;

    @AllArgsConstructor
    static class ZKWatcher implements Watcher {
        private final CompletableFuture<Void> sessionExpiryFuture;

        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == Event.KeeperState.Expired) {
                sessionExpiryFuture.complete(null);
            }
        }
    }

    public ControllerServiceMain(ControllerServiceConfig serviceConfig) {
        this.objectId = "ControllerServiceMain";
        this.serviceConfig = serviceConfig;
        this.staterInitialized = new CountDownLatch(1);
        this.serviceStopFuture = new CompletableFuture<>();
    }

    @Override
    protected void triggerShutdown() {
        this.serviceStopFuture.complete(null);
    }

    @Override
    protected void run() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, this.objectId, "run");
        try {
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
                    CompletableFuture<Void> sessionExpiryFuture = new CompletableFuture<>();
                    client.getZookeeperClient().getZooKeeper().register(new ZKWatcher(sessionExpiryFuture));

                    // At this point, wait until either of the two things happen
                    // 1. ZK session expires, i.e., sessionExpiryFuture completes, or
                    // 2. This ControllerServiceMain instance is stopped by invoking stopAsync() method,
                    //    i.e., serviceStopFuture completes.
                    CompletableFuture.anyOf(sessionExpiryFuture, this.serviceStopFuture).join();

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

                // Await termination of this ControllerServiceMain service.
                log.info("Awaiting termination of ControllerServiceMain");
                this.serviceStopFuture.join();

                // Initiate termination of controller services.
                log.info("Stopping controller services");
                starter.stopAsync();

                log.info("Awaiting termination of controller services");
                starter.awaitTerminated();
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
}
