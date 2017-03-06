/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.rpc;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;

/**
 * RPC server config.
 */
@Data
public class RPCServerConfig {
    private final int port;

    @Builder
    public RPCServerConfig(int port) {
        Preconditions.checkArgument(port > 0, "Invalid port.");

        this.port = port;
    }
}
