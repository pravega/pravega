/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import lombok.Getter;

/**
 * Configuration for the Attribute Index.
 */
public class AttributeIndexConfig {
    //region Config Names

    public static final Property<Integer> MAX_INDEX_PAGE_SIZE = Property.named("maxIndexPageSize", 32 * 1024);
    public static final Property<Integer> ATTRIBUTE_SEGMENT_ROLLING_SIZE = Property.named("attributeSegmentRollingSizeBytes", 4 * 1024 * 1024);
    private static final int MIN_INDEX_PAGE_SIZE = 1024;
    private static final String COMPONENT_CODE = "attributeindex";

    //endregion

    //region Members

    /**
     * The maximum index page size, in bytes.
     */
    @Getter
    private final int maxIndexPageSize;

    /**
     * The Attribute Segment Rolling Policy. If not explicitly defined in the configuration, it will be auto-calculated
     * based on the SnapshotTriggerSize and ReadBlockSize.
     */
    @Getter
    private final SegmentRollingPolicy attributeSegmentRollingPolicy;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AttributeIndexConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private AttributeIndexConfig(TypedProperties properties) throws ConfigurationException {
        int rollingSize = properties.getInt(ATTRIBUTE_SEGMENT_ROLLING_SIZE);
        if (rollingSize <= 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer; found '%d'.",
                    ATTRIBUTE_SEGMENT_ROLLING_SIZE, rollingSize));
        }
        this.attributeSegmentRollingPolicy = new SegmentRollingPolicy(rollingSize);

        this.maxIndexPageSize = properties.getInt(MAX_INDEX_PAGE_SIZE);
        if (this.maxIndexPageSize < MIN_INDEX_PAGE_SIZE) {
            throw new ConfigurationException(String.format("Property '%s' must at least %s; found '%d'.",
                    ATTRIBUTE_SEGMENT_ROLLING_SIZE, MIN_INDEX_PAGE_SIZE, rollingSize));
        }
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<AttributeIndexConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, AttributeIndexConfig::new);
    }
    //endregion
}
