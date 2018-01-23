/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client;

import com.google.common.annotations.Beta;
import io.pravega.client.stream.impl.PravegaCredentials;
import java.net.URI;
import lombok.Builder;
import lombok.Data;

/**
 * This class contains configuration that is passed on to Pravega client.
 * Please note that this is an experimental object and the contents and their interpretation may change
 * in future.
 */
@Data
@Builder
@Beta
public class PravegaClientConfig {
    /** controllerURI The controller rpc URI. This can be of 2 types
     1. tcp://ip1:port1,ip2:port2,...
        This is used if the controller endpoints are static and can be directly accessed.
     2. pravega://ip1:port1,ip2:port2,...
        This is used to autodiscovery the controller endpoints from an initial controller list.
    */
    private final URI controllerURI;

    /**
     * Flag to enable TLS. TODO: This can be read from the URL.
     */
    private final boolean enableTls;

    /**
     * Credentials to be passed on to the Pravega controller for authentication and authorization.
     */
    private final PravegaCredentials credentials;

    /**
     * Path to an optional truststore. If this is null or empty, the default JVM trust store is used.
     */
    private final String pravegaTrustStore;

    /**
     * If the flag {@link #enableTls is set, this flag decides whether to enable host name validation or not.
     */
    private boolean validateHostName;
}
