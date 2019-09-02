/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.impl;

import io.pravega.common.Exceptions;
import io.pravega.controller.server.ControllerServiceConfig;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.rest.RESTServerConfig;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.StoreType;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.Optional;

/**
 * Controller Service Configuration.
 */
@ToString
@Getter
public class ControllerServiceConfigImpl implements ControllerServiceConfig {

    private final int threadPoolSize;
    private final StoreClientConfig storeClientConfig;
    private final HostMonitorConfig hostMonitorConfig;
    private final boolean controllerClusterListenerEnabled;
    private final TimeoutServiceConfig timeoutServiceConfig;

    private final Optional<ControllerEventProcessorConfig> eventProcessorConfig;

    private final Optional<GRPCServerConfig> gRPCServerConfig;

    private final Optional<RESTServerConfig> restServerConfig;

    @Builder
    ControllerServiceConfigImpl(final int threadPoolSize,
                                final StoreClientConfig storeClientConfig,
                                final HostMonitorConfig hostMonitorConfig,
                                final boolean controllerClusterListenerEnabled,
                                final TimeoutServiceConfig timeoutServiceConfig,
                                final Optional<ControllerEventProcessorConfig> eventProcessorConfig,
                                final Optional<GRPCServerConfig> grpcServerConfig,
                                final Optional<RESTServerConfig> restServerConfig) {
        Exceptions.checkArgument(threadPoolSize > 0, "threadPoolSize", "Should be positive integer");
        Preconditions.checkNotNull(storeClientConfig, "storeClientConfig");
        Preconditions.checkNotNull(hostMonitorConfig, "hostMonitorConfig");
        Preconditions.checkNotNull(timeoutServiceConfig, "timeoutServiceConfig");
        Preconditions.checkNotNull(storeClientConfig, "storeClientConfig");
        Preconditions.checkNotNull(hostMonitorConfig, "hostMonitorConfig");
        if (controllerClusterListenerEnabled) {
            Preconditions.checkArgument(storeClientConfig.getStoreType() == StoreType.Zookeeper ||
                            storeClientConfig.getStoreType() == StoreType.PravegaTable,
                    "If controllerCluster is enabled, store type should be Zookeeper");
        }
        if (eventProcessorConfig.isPresent()) {
            Preconditions.checkNotNull(eventProcessorConfig.get());
        }
        if (grpcServerConfig.isPresent()) {
            Preconditions.checkNotNull(grpcServerConfig.get());
        }
        if (restServerConfig.isPresent()) {
            Preconditions.checkNotNull(restServerConfig.get());
        }

        this.threadPoolSize = threadPoolSize;
        this.storeClientConfig = storeClientConfig;
        this.hostMonitorConfig = hostMonitorConfig;
        this.controllerClusterListenerEnabled = controllerClusterListenerEnabled;
        this.timeoutServiceConfig = timeoutServiceConfig;
        this.eventProcessorConfig = eventProcessorConfig;
        this.gRPCServerConfig = grpcServerConfig;
        this.restServerConfig = restServerConfig;
    }
}
