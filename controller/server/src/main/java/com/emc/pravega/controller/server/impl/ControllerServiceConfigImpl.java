/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.impl;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.controller.server.ControllerServiceConfig;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import com.emc.pravega.controller.server.rest.RESTServerConfig;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import com.emc.pravega.controller.store.client.StoreClientConfig;
import com.emc.pravega.controller.store.host.HostMonitorConfig;
import com.emc.pravega.controller.timeout.TimeoutServiceConfig;
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
    private final TimeoutServiceConfig timeoutServiceConfig;

    private final Optional<ControllerEventProcessorConfig> eventProcessorConfig;
    private final boolean requestHandlersEnabled;

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
                            final TimeoutServiceConfig timeoutServiceConfig,
                            final Optional<ControllerEventProcessorConfig> eventProcessorConfig,
                            final boolean requestHandlersEnabled,
                            final Optional<GRPCServerConfig> grpcServerConfig,
                            final Optional<RESTServerConfig> restServerConfig) {
        Exceptions.checkArgument(serviceThreadPoolSize > 0, "serviceThreadPoolSize", "Should be positive integer");
        Exceptions.checkArgument(taskThreadPoolSize > 0, "taskThreadPoolSize", "Should be positive integer");
        Exceptions.checkArgument(storeThreadPoolSize > 0, "storeThreadPoolSize", "Should be positive integer");
        Exceptions.checkArgument(eventProcThreadPoolSize > 0, "eventProcThreadPoolSize", "Should be positive integer");
        Exceptions.checkArgument(requestHandlerThreadPoolSize > 0, "requestHandlerThreadPoolSize", "Should be positive integer");
        Preconditions.checkNotNull(storeClientConfig, "storeClientConfig");
        Preconditions.checkNotNull(hostMonitorConfig, "hostMonitorConfig");
        Preconditions.checkNotNull(timeoutServiceConfig, "timeoutServiceConfig");
        Preconditions.checkNotNull(storeClientConfig, "storeClientConfig");
        Preconditions.checkNotNull(hostMonitorConfig, "hostMonitorConfig");
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
        this.timeoutServiceConfig = timeoutServiceConfig;
        this.eventProcessorConfig = eventProcessorConfig;
        this.requestHandlersEnabled = requestHandlersEnabled;
        this.gRPCServerConfig = grpcServerConfig;
        this.restServerConfig = restServerConfig;
    }
}
