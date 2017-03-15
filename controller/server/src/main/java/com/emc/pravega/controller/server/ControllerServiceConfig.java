/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.controller.server.rest.RESTServerConfig;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.timeout.TimeoutServiceConfig;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;

/**
 * Controller Service Configuration.
 */
@Getter
public class ControllerServiceConfig {

    @Getter
    public static class HostMonitorConfig {
        private final boolean hostMonitorEnabled;
        private final int hostMonitorMinRebalanceInterval;
        private final String sssHost;
        private final int sssPort;
        private final int containerCount;

        @Builder
        public HostMonitorConfig(final boolean hostMonitorEnabled,
                                 final int hostMonitorMinRebalanceInterval,
                                 final String sssHost,
                                 final int sssPort,
                                 final int containerCount) {
            Exceptions.checkArgument(hostMonitorMinRebalanceInterval > 0, "hostMonitorMinRebalanceInterval",
                    "Should be positive integer");
            if (!hostMonitorEnabled) {
                Exceptions.checkNotNullOrEmpty(sssHost, "ssshost");
                Exceptions.checkArgument(sssPort > 0, "sssPort", "Should be positive integer");
                Exceptions.checkArgument(containerCount > 0, "containerCount", "Should be positive integer");
            }
            this.hostMonitorEnabled = hostMonitorEnabled;
            this.hostMonitorMinRebalanceInterval = hostMonitorMinRebalanceInterval;
            this.sssHost = sssHost;
            this.sssPort = sssPort;
            this.containerCount = containerCount;
        }
    }

    private final String host;
    private final int threadPoolSize;
    private final StoreClient storeClient;
    private final HostMonitorConfig hostMonitorConfig;
    private final TimeoutServiceConfig timeoutServiceConfig;

    private final boolean eventProcessorsEnabled;
    private final boolean requestHandlersEnabled;

    private final boolean gRPCServerEnabled;
    private final GRPCServerConfig gRPCServerConfig;

    private final boolean restServerEnabled;
    private final RESTServerConfig restServerConfig;

    @Builder
    ControllerServiceConfig(final String host,
                            final int threadPoolSize,
                            final StoreClient storeClient,
                            final HostMonitorConfig hostMonitorConfig,
                            final TimeoutServiceConfig timeoutServiceConfig,
                            final boolean eventProcessorsEnabled,
                            final boolean requestHandlersEnabled,
                            final boolean gRPCServerEnabled,
                            final GRPCServerConfig grpcServerConfig,
                            final boolean restServerEnabled,
                            final RESTServerConfig restServerConfig) {
        Exceptions.checkNotNullOrEmpty(host, "host");
        Exceptions.checkArgument(threadPoolSize > 0, "threadPoolSize", "Should be positive integer");
        Preconditions.checkNotNull(storeClient, "storeClient");
        Preconditions.checkNotNull(hostMonitorConfig, "hostMonitorConfig");

        if (gRPCServerEnabled) {
            Preconditions.checkNotNull(grpcServerConfig);
        }
        if (restServerEnabled) {
            Preconditions.checkNotNull(restServerConfig);
        }
        this.host = host;
        this.threadPoolSize = threadPoolSize;
        this.storeClient = storeClient;
        this.hostMonitorConfig = hostMonitorConfig;
        this.timeoutServiceConfig = timeoutServiceConfig;
        this.eventProcessorsEnabled = eventProcessorsEnabled;
        this.requestHandlersEnabled = requestHandlersEnabled;
        this.gRPCServerEnabled = gRPCServerEnabled;
        this.gRPCServerConfig = grpcServerConfig;
        this.restServerEnabled = restServerEnabled;
        this.restServerConfig = restServerConfig;
    }
}
