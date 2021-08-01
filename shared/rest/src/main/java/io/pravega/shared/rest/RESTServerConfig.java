/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.rest;

import io.pravega.auth.ServerConfig;

/**
 * Configuration of controller REST server.
 */
public interface RESTServerConfig extends ServerConfig {
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
     * Version for the TLS protocol.
     * @return TLS protocol Version is specified.
     */
    String[] tlsProtocolVersion();

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
