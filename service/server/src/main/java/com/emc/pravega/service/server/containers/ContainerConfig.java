/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;
import java.time.Duration;
import java.util.Properties;
import lombok.Getter;

/**
 * Segment Container Configuration.
 */
public class ContainerConfig extends ComponentConfig {
    //region Members

    public static final String COMPONENT_CODE = "containers";
    public static final String PROPERTY_SEGMENT_METADATA_EXPIRATION_SECONDS = "segmentMetadataExpirationSeconds";

    public static final int MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS = 60; // Minimum possible value for segmentExpiration
    private static final int DEFAULT_SEGMENT_METADATA_EXPIRATION_SECONDS = 5 * 60; // 5 Minutes.

    /**
     * The amount of time after which Segments are eligible for eviction from the metadata.
     */
    @Getter
    private Duration segmentMetadataExpiration;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws ConfigurationException   When a configuration issue has been detected. This can be:
     *                                  MissingPropertyException (a required Property is missing from the given properties collection),
     *                                  NumberFormatException (a Property has a value that is invalid for it).
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string..
     */
    public ContainerConfig(Properties properties) throws ConfigurationException {
        super(properties, COMPONENT_CODE);
    }

    //endregion

    //region ComponentConfig Implementation

    @Override
    protected void refresh() throws ConfigurationException {
        int segmentMetadataExpirationSeconds = getInt32Property(PROPERTY_SEGMENT_METADATA_EXPIRATION_SECONDS, DEFAULT_SEGMENT_METADATA_EXPIRATION_SECONDS);
        if (segmentMetadataExpirationSeconds < MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS) {
            throw new ConfigurationException(String.format("Property '%s' must be at least %s.", PROPERTY_SEGMENT_METADATA_EXPIRATION_SECONDS, MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS));
        }
        this.segmentMetadataExpiration = Duration.ofSeconds(segmentMetadataExpirationSeconds);
    }

    //endregion
}
