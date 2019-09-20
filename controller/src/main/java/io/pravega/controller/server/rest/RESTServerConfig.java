/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rest;

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

    /**
     * Flag which denotes whether TLS is enabled.
     * @return Flag which denotes whether TLS is enabled.
     */
    boolean isTlsEnabled();

    /**
     * Path to a file which contains the key file for the TLS connection.
     * @return File which contains the key file for the TLS connection.
     */
    String getKeyFilePath();

    /**
     * File which contains the password for the key file for the TLS connection.
     * @return File which contains the password for the key file for the TLS connection.
     */
    String getKeyFilePasswordPath();
}
