/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.rpc;

import lombok.Builder;
import lombok.Data;

/**
 * Thrift RPC server config.
 */
@Data
@Builder
public class RPCServerConfig {
    int port;
    int workerThreadCount;
    int selectorThreadCount;
    int maxReadBufferBytes;
}
