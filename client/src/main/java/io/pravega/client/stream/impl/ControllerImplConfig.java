/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.ClientConfig;
import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ControllerImplConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int initialBackoffMillis;
    private final int maxBackoffMillis;
    private final int retryAttempts;
    private final int backoffMultiple;
    private final ClientConfig clientConfig;

    public static final class ControllerImplConfigBuilder {
        private int initialBackoffMillis = 1;
        private int maxBackoffMillis = 20000;
        private int retryAttempts = 10;
        private int backoffMultiple = 10;
        private ClientConfig config = ClientConfig.builder().controllerURI(null)
                                                  .credentials(null).trustStore("").build();
    }
}