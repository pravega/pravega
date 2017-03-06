/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.rpc;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;

/**
 * Thrift RPC server config.
 */
@Data
public class RPCServerConfig {
    private final int port;
    private final int workerThreadCount;
    private final int selectorThreadCount;
    private final int maxReadBufferBytes;

    @Builder
    public RPCServerConfig(int port, int workerThreadCount, int selectorThreadCount, int maxReadBufferBytes) {
        Preconditions.checkArgument(port > 0, "Invalid port.");
        Preconditions.checkArgument(workerThreadCount > 0, "Invalid workerThreadCount.");
        Preconditions.checkArgument(selectorThreadCount > 0, "Invalid selectorThreadCount.");
        Preconditions.checkArgument(maxReadBufferBytes > 0, "Invalid maxReadBufferBytes.");

        this.port = port;
        this.workerThreadCount = workerThreadCount;
        this.selectorThreadCount = selectorThreadCount;
        this.maxReadBufferBytes = maxReadBufferBytes;
    }
}
