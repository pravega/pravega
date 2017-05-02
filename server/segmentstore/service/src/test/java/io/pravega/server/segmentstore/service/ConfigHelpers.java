/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.segmentstore.service;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.server.segmentstore.service.reading.ReadIndexConfig;

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
