/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.rpc.grpc.impl;

import com.emc.pravega.shared.Exceptions;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;

/**
 * gRPC server config.
 */
@Data
public class GRPCServerConfigImpl implements GRPCServerConfig {
    private final int port;
    private final String publishedRPCHost;
    private final int publishedRPCPort;

    @Builder
    public GRPCServerConfigImpl(final int port, final String publishedRPCHost, final int publishedRPCPort) {
        Preconditions.checkArgument(port > 0, "Invalid port.");
        Exceptions.checkNotNullOrEmpty(publishedRPCHost, "publishedRPCHost");
        Preconditions.checkArgument(publishedRPCPort > 0, "publishedRPCPort should be a positive integer");

        this.port = port;
        this.publishedRPCHost = publishedRPCHost;
        this.publishedRPCPort = publishedRPCPort;
    }
}
