/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.server.rpc.grpc;

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

    /**
     * Fetches the RPC address which has to be registered with the cluster used for external access.
     *
     * @return The RPC address which has to be registered with the cluster used for external access.
     */
    String getPublishedRPCHost();

    /**
     * Fetches the RPC port which has to be registered with the cluster used for external access.
     *
     * @return The RPC port which has to be registered with the cluster used for external access.
     */
    int getPublishedRPCPort();
}
