/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.rest;

/**
 * Configuration of controller REST server.
 */
public interface RESTServerConfig {
    /**
     * Fetches the host ip address to which the controller gRPC server binds.
     *
     * @return The host ip address to which the controller gRPC server binds.
     */
    String getHost();

    /**
     * Fetches the port on which controller gRPC listens.
     *
     * @return The port on which controller gRPC listens.
     */
    int getPort();
}
