/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.rpc.grpc;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;

/**
 * gRPC server config.
 */
@Data
public class GRPCServerConfig {
    private final int port;

    @Builder
    public GRPCServerConfig(int port) {
        Preconditions.checkArgument(port > 0, "Invalid port.");

        this.port = port;
    }
}
