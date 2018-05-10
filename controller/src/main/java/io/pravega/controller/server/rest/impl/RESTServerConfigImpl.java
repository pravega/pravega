/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rest.impl;

import io.pravega.common.Exceptions;
import io.pravega.controller.server.rest.RESTServerConfig;
import lombok.Builder;
import lombok.Getter;

/**
 * REST server config.
 */
@Getter
public class RESTServerConfigImpl implements RESTServerConfig {
    private final String host;
    private final int port;
    private final boolean enableTls;
    private final String keyFilePath;
    private final String keyFilePasswordPath;

    @Builder
    RESTServerConfigImpl(final String host, final int port, boolean enableTls, String keyFilePath, String keyFilePasswordPath) {
        Exceptions.checkNotNullOrEmpty(host, "host");
        Exceptions.checkArgument(port > 0, "port", "Should be positive integer");

        this.host = host;
        this.port = port;
        this.enableTls = enableTls;
        this.keyFilePath = keyFilePath;
        this.keyFilePasswordPath = keyFilePasswordPath;
    }
}
