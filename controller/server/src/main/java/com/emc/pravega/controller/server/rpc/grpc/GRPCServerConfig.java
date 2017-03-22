/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.rpc.grpc;

/**
 * Configuration of controller gRPC server.
 */
public interface GRPCServerConfig {
    /**
     * Fetches the port on which controller gRPC server listens.
     *
     * @return the port on which controller gRPC server listens.
     */
    int getPort();
}
