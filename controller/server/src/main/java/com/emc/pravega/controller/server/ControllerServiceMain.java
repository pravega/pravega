/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.shared.LoggerHelpers;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.client.StoreClientFactory;
import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Monitor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * ControllerServiceMonitor, entry point into the controller service.
 */
@Slf4j
public class ControllerServiceMain extends AbstractExecutionThreadService {

    enum ServiceState {
        NEW,
        STARTING,
        PAUSING,
    }

    private final String objectId;
    private final ControllerServiceConfig serviceConfig;
    private final BiFunction<ControllerServiceConfig, StoreClient, ControllerServiceStarter> starterFactory;
    private ControllerServiceStarter starter;
    private final CompletableFuture<Void> serviceStopFuture;
    private StoreClient storeClient;

    private ServiceState serviceState;
    private final Monitor monitor = new Monitor();
    private final Monitor.Guard hasReachedStarting = new HasReachedState(ServiceState.STARTING);
    private final Monitor.Guard hasReachedPausing = new HasReachedState(ServiceState.PAUSING);


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

    final class HasReachedState extends Monitor.Guard {
        private ServiceState desiredState;

        HasReachedState(ServiceState desiredState) {
            super(monitor);
            this.desiredState = desiredState;
        }

        @Override
        public boolean isSatisfied() {
            return serviceState == desiredState;
        }
    }

    public ControllerServiceMain(ControllerServiceConfig serviceConfig) {
        this(serviceConfig, ControllerServiceStarter::new);
    }

    @VisibleForTesting
    ControllerServiceMain(final ControllerServiceConfig serviceConfig,
                          final BiFunction<ControllerServiceConfig, StoreClient, ControllerServiceStarter> starterFactory) {
        this.objectId = "ControllerServiceMain";
        this.serviceConfig = serviceConfig;
        this.starterFactory = starterFactory;
        this.serviceStopFuture = new CompletableFuture<>();
        this.serviceState = ServiceState.NEW;
    }

    @Override
    protected void triggerShutdown() {
        log.info("Shutting down ControllerServiceMain");
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
                    storeClient = StoreClientFactory.createStoreClient(serviceConfig.getStoreClientConfig());

                    // Client should be ZK if controller cluster is enabled.
                    CuratorFramework client = (CuratorFramework) storeClient.getClient();

                    log.info("Awaiting ZK client connection to ZK server");
                    client.blockUntilConnected();

                    // Start controller services.
                    starter = starterFactory.apply(serviceConfig, storeClient);
                    log.info("Starting controller services");
                    notifyServiceStateChange(ServiceState.STARTING);
                    starter.startAsync();

                    // Await ZK session expiry.
                    log.info("Awaiting ZK session expiry or termination trigger for ControllerServiceMain");
                    CompletableFuture<Void> sessionExpiryFuture = new CompletableFuture<>();
                    client.getZookeeperClient().getZooKeeper().register(new ZKWatcher(sessionExpiryFuture));

                    log.info("Awaiting controller services start");
                    starter.awaitRunning();

                    // At this point, wait until either of the two things happen
                    // 1. ZK session expires, i.e., sessionExpiryFuture completes, or
                    // 2. This ControllerServiceMain instance is stopped by invoking stopAsync() method,
                    //    i.e., serviceStopFuture completes.
                    CompletableFuture.anyOf(sessionExpiryFuture, this.serviceStopFuture).join();

                    // Problem of curator automatically recreating ZK client on session expiry is mitigated by
                    // employing a custom ZookeeperFactory that always returns the same ZK client to curator

                    // Once ZK session expires or once ControllerServiceMain is externally stopped,
                    // stop ControllerServiceStarter.
                    if (sessionExpiryFuture.isDone()) {
                        log.info("ZK session expired");
                        storeClient.close();
                    }

                    log.info("Stopping ControllerServiceStarter");
                    notifyServiceStateChange(ServiceState.PAUSING);
                    starter.stopAsync();

                    log.info("Awaiting termination of ControllerServiceStarter");
                    starter.awaitTerminated();
                }
            } else {
                // Create store client.
                log.info("Creating store client");
                storeClient = StoreClientFactory.createStoreClient(serviceConfig.getStoreClientConfig());

                // Start controller services.
                starter = starterFactory.apply(serviceConfig, storeClient);
                log.info("Starting controller services");
                notifyServiceStateChange(ServiceState.STARTING);
                starter.startAsync();

                log.info("Awaiting controller services start");
                starter.awaitRunning();

                // Await termination of this ControllerServiceMain service.
                log.info("Awaiting termination trigger for ControllerServiceMain");
                this.serviceStopFuture.join();

                // Initiate termination of controller services.
                log.info("Stopping controller services");
                notifyServiceStateChange(ServiceState.PAUSING);
                starter.stopAsync();

                log.info("Awaiting termination of controller services");
                starter.awaitTerminated();
            }
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "run", traceId);
        }
    }

    /**
     * Changes internal state to the new value.
     * @param newState new internal state.
     */
    private void notifyServiceStateChange(ServiceState newState) {
        monitor.enter();
        try {
            serviceState = newState;
        } finally {
            monitor.leave();
        }
    }

    /**
     * Awaits until the internal state changes to STARTING, and returns the reference
     * of current ControllerServiceStarter.
     */
    @VisibleForTesting
    public ControllerServiceStarter awaitServiceStarting() {
        monitor.enterWhenUninterruptibly(hasReachedStarting);
        try {
            if (serviceState != ServiceState.STARTING) {
                throw new IllegalStateException("Expected state=" + ServiceState.STARTING +
                        ", but actual state=" + serviceState);
            } else {
                return this.starter;
            }
        } finally {
            monitor.leave();
        }
    }

    /**
     * Awaits until the internal state changes to PAUSING, and returns the reference
     * of current ControllerServiceStarter.
     */
    @VisibleForTesting
    public ControllerServiceStarter awaitServicePausing() {
        monitor.enterWhenUninterruptibly(hasReachedPausing);
        try {
            if (serviceState != ServiceState.PAUSING) {
                throw new IllegalStateException("Expected state=" + ServiceState.PAUSING +
                        ", but actual state=" + serviceState);
            } else {
                return this.starter;
            }
        } finally {
            monitor.leave();
        }
    }

    @VisibleForTesting
    public void forceClientSessionExpiry() throws Exception {
        Preconditions.checkState(serviceConfig.getControllerClusterListenerConfig().isPresent(),
                "Controller Cluster not enabled");
        awaitServiceStarting();
        ZKUtils.simulateZkSessionExpiry((CuratorFramework) this.storeClient.getClient());
    }
}
