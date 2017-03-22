/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.rest.impl;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.controller.server.rest.RESTServerConfig;
import lombok.Builder;
import lombok.Getter;

/**
 * REST server config.
 */
@Getter
public class RESTServerConfigImpl implements RESTServerConfig {
    private final String host;
    private final int port;

    @Builder
    RESTServerConfigImpl(final String host, final int port) {
        Exceptions.checkNotNullOrEmpty(host, "host");
        Exceptions.checkArgument(port > 0, "port", "Should be positive integer");

        this.host = host;
        this.port = port;
    }
}
