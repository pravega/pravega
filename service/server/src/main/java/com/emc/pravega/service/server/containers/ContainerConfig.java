/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.Property;
import com.emc.pravega.common.util.TypedProperties;
import java.time.Duration;
import lombok.Getter;

/**
 * Segment Container Configuration.
 */
public class ContainerConfig {
    //region Members

    public static final int MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS = 60; // Minimum possible value for segmentExpiration
    public static final Property<Integer> SEGMENT_METADATA_EXPIRATION_SECONDS = Property.named("segmentMetadataExpirationSeconds",
            5 * MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS);
    public static final Property<Integer> MAX_ACTIVE_SEGMENT_COUNT = Property.named("maxActiveSegmentCount", 10000);
    private static final String COMPONENT_CODE = "containers";

    /**
     * The amount of time after which Segments are eligible for eviction from the metadata.
     */
    @Getter
    private Duration segmentMetadataExpiration;

    /**
     * The maximum number of segments that can be active at any given time in a container.
     */
    @Getter
    private int maxActiveSegmentCount;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     */
    ContainerConfig(TypedProperties properties) {
        int segmentMetadataExpirationSeconds = properties.getInt(SEGMENT_METADATA_EXPIRATION_SECONDS);
        if (segmentMetadataExpirationSeconds < MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS) {
            throw new ConfigurationException(String.format("Property '%s' must be at least %s.",
                    SEGMENT_METADATA_EXPIRATION_SECONDS, MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS));
        }
        this.segmentMetadataExpiration = Duration.ofSeconds(segmentMetadataExpirationSeconds);
        int maxActiveSegments = properties.getInt(MAX_ACTIVE_SEGMENT_COUNT);
        if (maxActiveSegments < 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer.", MAX_ACTIVE_SEGMENT_COUNT));
        }
        this.maxActiveSegmentCount = maxActiveSegments;
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ContainerConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ContainerConfig::new);
    }

    //endregion
}
