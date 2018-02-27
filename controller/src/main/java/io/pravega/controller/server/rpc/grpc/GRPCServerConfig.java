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
     * Fetches the settings which indicates whether authorization is enabled.
     *
     * @return Whether this deployment has auth enabled.
     */
    boolean isAuthorizationEnabled();

    /**
     * Fetches the location of passwd file for the users.
     * This uses UNIX password file syntax. It is used for default implementation of authorization
     * and authentication.
     *
     * @return Location of password file.
     */
    String getUserPasswdFile();

    /**
     * A configuration to switch TLS on client to controller interactions.
      * @return A flag representing TLS status.
     */
    boolean isTlsEnabled();

    /**
     * X.509 certificate file to be used for TLS.
     *
     * @return A file which contains the TLS certificate.
     */
    String getTlsCertFile();

    /**
     * File containing the private key for the X.509 certificate used for TLS.
     * @return A file which contains the private key for the TLS certificate.
     */
    String getTlsKeyFile();
}
