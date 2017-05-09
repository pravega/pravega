/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.server;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.service.server.reading.ReadIndexConfig;

/**
 * Helper class that can be used to quickly create Configurations.
 */
public class ConfigHelpers {
    /**
     * Updates the given builder to have an infinite cache policy.
     *
     * @param builder The properties to include.
     */
    public static ConfigBuilder<ReadIndexConfig> withInfiniteCachePolicy(ConfigBuilder<ReadIndexConfig> builder) {
        return builder
                .with(ReadIndexConfig.CACHE_POLICY_MAX_SIZE, Long.MAX_VALUE)
                .with(ReadIndexConfig.CACHE_POLICY_MAX_TIME, Integer.MAX_VALUE)
                .with(ReadIndexConfig.CACHE_POLICY_GENERATION_TIME, Integer.MAX_VALUE);
    }
}
