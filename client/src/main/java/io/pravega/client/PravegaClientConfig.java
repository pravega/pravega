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

import io.pravega.client.stream.impl.PravegaCredentials;
import java.net.URI;
import lombok.Builder;
import lombok.Data;

/**
 * This class contains configuration that is passed on to Pravega client.
 */
@Data
@Builder
public class PravegaClientConfig {
    /** controllerURI The controller rpc URI. This can be of 2 types
     1. tcp://ip1:port1,ip2:port2,...
        This is used if the controller endpoints are static and can be directly accessed.
     2. pravega://ip1:port1,ip2:port2,...
        This is used to autodiscovery the controller endpoints from an initial controller list.
    */
    private final URI controllerURI;
    private final boolean enableTls;
    private final PravegaCredentials credentials;
    private final String pravegaTrustStore;
}
