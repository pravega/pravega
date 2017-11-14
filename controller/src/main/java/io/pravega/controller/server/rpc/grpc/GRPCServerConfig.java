/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.grpc;

import java.util.Optional;

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
     * Fetches the settings which indicates whether authorization is enabled.
     *
     * @return Whether this deployment has auth enabled.
     */
    boolean isAuthorizationEnabled();

    /**
     * Fetches the RPC address which has to be registered with the cluster used for external access.
     *
     * @return The RPC address which has to be registered with the cluster used for external access.
     */
    Optional<String> getPublishedRPCHost();

    /**
     * Fetches the RPC port which has to be registered with the cluster used for external access.
     *
     * @return The RPC port which has to be registered with the cluster used for external access.
     */
    Optional<Integer> getPublishedRPCPort();

    /**
     * Fetches the list of users in the user database currently stored in the config.
     * @return Comma separated list of users.
     */
    String getUsers();

    /**
     * Fetches list of password tokens for the users.
     * @return Comma separated list of passwords.
     */
    String getPasswords();

    /**
     * Fetches the URL for guardian server.
     * @return URL of the guardian server.
     */
    String getGuardianIP();
}
