/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.client.stream.StreamConfiguration;
import lombok.Data;

import java.io.Serializable;

@Data
public class StreamConfigWithVersion implements Serializable {

    private final StreamConfiguration configuration;

    private final int version;

    public static StreamConfigWithVersion generateNext(final StreamConfigWithVersion previous,
                                                       final StreamConfiguration newConfig) {
        return new StreamConfigWithVersion(newConfig, previous.getVersion() + 1);
    }
}