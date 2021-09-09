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
package io.pravega.controller.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Monitor;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.function.Callbacks;
import io.pravega.controller.metrics.ZookeeperMetrics;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.client.StoreType;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;

/**
 * ControllerServiceMonitor, entry point into the controller service.
 */
@Slf4j
public class ControllerServiceMain extends AbstractExecutionThreadService implements AutoCloseable {

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

    private final ZookeeperMetrics zookeeperMetrics;

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
        this.zookeeperMetrics = new ZookeeperMetrics();
    }

    @Override
    protected void triggerShutdown() {
        log.info("Shutting down Controller Service.");
        this.serviceStopFuture.complete(null);
    }

    @Override
    protected void run() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, this.objectId, "run");
        try {
            while (isRunning()) {
                // Create store client.
                log.debug("Creating store client");
                storeClient = StoreClientFactory.createStoreClient(serviceConfig.getStoreClientConfig());

                starter = starterFactory.apply(serviceConfig, storeClient);

                boolean hasZkConnection = serviceConfig.getStoreClientConfig().getStoreType().equals(StoreType.Zookeeper) ||
                        serviceConfig.isControllerClusterListenerEnabled();

                CompletableFuture<Void> sessionExpiryFuture = new CompletableFuture<>();
                if (hasZkConnection) {
                    CuratorFramework client = (CuratorFramework) storeClient.getClient();

                    log.debug("Awaiting ZK client connection to ZK server");
                    client.blockUntilConnected();

                    // Await ZK session expiry.
                    log.debug("Awaiting ZK session expiry or termination trigger for ControllerServiceMain");
                    client.getConnectionStateListenable().addListener((client1, newState) -> {
                        if (newState.equals(ConnectionState.LOST)) {
                            sessionExpiryFuture.complete(null);
                            starter.notifySessionExpiration();
                        }
                    });
                }

                // Start controller services.
                log.info("Starting Controller Services.");
                notifyServiceStateChange(ServiceState.STARTING);
                starter.startAsync();
                starter.awaitRunning();
                log.info("Controller Services started successfully.");
                if (hasZkConnection) {
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
                        zookeeperMetrics.reportZKSessionExpiration();
                        log.info("ZK session expired. Stopping Controller Services.");
                    }
                } else {
                    this.serviceStopFuture.join();
                    log.info("Stopping Controller Services.");
                }

                notifyServiceStateChange(ServiceState.PAUSING);
                starter.stopAsync();

                log.debug("Awaiting termination of ControllerServices");
                starter.awaitTerminated();

                if (hasZkConnection) {
                    log.debug("Calling close on store client.");
                    storeClient.close();
                }
                log.info("Controller Services terminated successfully.");
            }
        } catch (Exception e) {
            log.error("Controller Service Main thread exited exceptionally", e);
            throw e;
        } finally {
            if (storeClient != null) {
                storeClient.close();
            }
            LoggerHelpers.traceLeave(log, this.objectId, "run", traceId);
        }
    }

    /**
     * Changes internal state to the new value.
     *
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
        Preconditions.checkState(serviceConfig.isControllerClusterListenerEnabled(),
                "Controller Cluster not enabled");
        awaitServiceStarting();
        ((CuratorFramework) this.storeClient.getClient()).getZookeeperClient().getZooKeeper()
                                                         .getTestable().injectSessionExpiration();
    }

    @Override
    protected void shutDown() throws Exception {
        if (starter != null) {
            if (starter.isRunning()) {
                triggerShutdown();
                starter.awaitTerminated();
            }
        }
        if (storeClient != null) {
            storeClient.close();
        }
    }

    @Override
    public void close() {
        if (starter != null) {
            triggerShutdown();
            Callbacks.invokeSafely(starter::close, ex -> log.debug("Error closing starter. " + ex.getMessage()));
        }

        if (storeClient != null) {
            Callbacks.invokeSafely(storeClient::close, ex -> log.debug("Error closing storeClient. " + ex.getMessage()));
        }
    }
}