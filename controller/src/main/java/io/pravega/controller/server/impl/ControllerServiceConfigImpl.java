/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.impl;

import io.pravega.common.Exceptions;
import io.pravega.controller.fault.ControllerClusterListenerConfig;
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

import java.util.Optional;

/**
 * Controller Service Configuration.
 */
@Getter
public class ControllerServiceConfigImpl implements ControllerServiceConfig {

    private final int serviceThreadPoolSize;
    private final int taskThreadPoolSize;
    private final int storeThreadPoolSize;
    private final int eventProcThreadPoolSize;
    private final int requestHandlerThreadPoolSize;
    private final StoreClientConfig storeClientConfig;
    private final HostMonitorConfig hostMonitorConfig;
    private final Optional<ControllerClusterListenerConfig> controllerClusterListenerConfig;
    private final TimeoutServiceConfig timeoutServiceConfig;

    private final Optional<ControllerEventProcessorConfig> eventProcessorConfig;

    private final Optional<GRPCServerConfig> gRPCServerConfig;

    private final Optional<RESTServerConfig> restServerConfig;

    @Builder
    ControllerServiceConfigImpl(final int serviceThreadPoolSize,
                            final int taskThreadPoolSize,
                            final int storeThreadPoolSize,
                            final int eventProcThreadPoolSize,
                            final int requestHandlerThreadPoolSize,
                            final StoreClientConfig storeClientConfig,
                            final HostMonitorConfig hostMonitorConfig,
                            final Optional<ControllerClusterListenerConfig> controllerClusterListenerConfig,
                            final TimeoutServiceConfig timeoutServiceConfig,
                            final Optional<ControllerEventProcessorConfig> eventProcessorConfig,
                            final Optional<GRPCServerConfig> grpcServerConfig,
                            final Optional<RESTServerConfig> restServerConfig) {
        Exceptions.checkArgument(serviceThreadPoolSize > 0, "serviceThreadPoolSize", "Should be positive integer");
        Exceptions.checkArgument(taskThreadPoolSize > 0, "taskThreadPoolSize", "Should be positive integer");
        Exceptions.checkArgument(storeThreadPoolSize > 0, "storeThreadPoolSize", "Should be positive integer");
        Exceptions.checkArgument(eventProcThreadPoolSize > 0, "eventProcThreadPoolSize", "Should be positive integer");
        Exceptions.checkArgument(requestHandlerThreadPoolSize > 0, "requestHandlerThreadPoolSize", "Should be positive integer");
        Preconditions.checkNotNull(storeClientConfig, "storeClientConfig");
        Preconditions.checkNotNull(hostMonitorConfig, "hostMonitorConfig");
        Preconditions.checkNotNull(controllerClusterListenerConfig, "controllerClusterListenerConfig");
        Preconditions.checkNotNull(timeoutServiceConfig, "timeoutServiceConfig");
        Preconditions.checkNotNull(storeClientConfig, "storeClientConfig");
        Preconditions.checkNotNull(hostMonitorConfig, "hostMonitorConfig");
        if (controllerClusterListenerConfig.isPresent()) {
            Preconditions.checkArgument(storeClientConfig.getStoreType() == StoreType.Zookeeper,
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

        this.serviceThreadPoolSize = serviceThreadPoolSize;
        this.taskThreadPoolSize = taskThreadPoolSize;
        this.storeThreadPoolSize = storeThreadPoolSize;
        this.eventProcThreadPoolSize = eventProcThreadPoolSize;
        this.requestHandlerThreadPoolSize = requestHandlerThreadPoolSize;
        this.storeClientConfig = storeClientConfig;
        this.hostMonitorConfig = hostMonitorConfig;
        this.controllerClusterListenerConfig = controllerClusterListenerConfig;
        this.timeoutServiceConfig = timeoutServiceConfig;
        this.eventProcessorConfig = eventProcessorConfig;
        this.gRPCServerConfig = grpcServerConfig;
        this.restServerConfig = restServerConfig;
    }
}
