/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.server.rest.impl;

import io.pravega.common.Exceptions;
import io.pravega.server.controller.service.server.rest.RESTServerConfig;
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
