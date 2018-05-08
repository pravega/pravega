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

import io.pravega.common.io.serialization.RevisionDataOutput;
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

    public static final Property<Integer> UPDATE_COUNT_THRESHOLD_SNAPSHOT = Property.named("updateCountThresholdForSnapshot", 50 * 1000);
    public static final Property<Integer> READ_BLOCK_SIZE = Property.named("readBlockSize", 1024 * 1024);
    private static final int AUTO_VALUE = -1; // This implies the value will be auto-calculated.
    public static final Property<Integer> ATTRIBUTE_SEGMENT_ROLLING_SIZE = Property.named("attributeSegmentRollingSizeBytes", AUTO_VALUE);
    private static final String COMPONENT_CODE = "attributeindex";
    private static final int ESTIMATED_ATTRIBUTE_SERIALIZATION_SIZE = RevisionDataOutput.UUID_BYTES + Long.BYTES;

    //endregion

    //region Members

    /**
     * The maximum read request length (bytes) to use when reading from Storage. This is also used as a minimum bound for
     * the Attribute Segment Rolling Policy if Auto-calculation was requested.
     */
    @Getter
    private final int readBlockSize;
    /**
     * The number of bytes after the end of the last Snapshot when to trigger a new Snapshot.
     */
    @Getter
    private final int snapshotTriggerSize;
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
        int updateCountThresholdSnapshot = properties.getInt(UPDATE_COUNT_THRESHOLD_SNAPSHOT);
        if (updateCountThresholdSnapshot <= 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer; found '%d'.", UPDATE_COUNT_THRESHOLD_SNAPSHOT, updateCountThresholdSnapshot));
        }

        this.snapshotTriggerSize = (int) Math.min(Integer.MAX_VALUE, (long) updateCountThresholdSnapshot * ESTIMATED_ATTRIBUTE_SERIALIZATION_SIZE);

        this.readBlockSize = properties.getInt(READ_BLOCK_SIZE);
        if (this.readBlockSize <= 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer; found '%d'.", READ_BLOCK_SIZE, this.readBlockSize));
        }

        this.attributeSegmentRollingPolicy = createRollingPolicy(this.snapshotTriggerSize, this.readBlockSize, properties);
    }

    private SegmentRollingPolicy createRollingPolicy(int snapshotTriggerSize, int readBlockSize, TypedProperties properties) {
        int configValue = properties.getInt(ATTRIBUTE_SEGMENT_ROLLING_SIZE);
        int rollingSize;
        if (configValue == AUTO_VALUE) {
            // We allow at least one whole snapshot inside a segment chunk.
            rollingSize = Math.max(readBlockSize, snapshotTriggerSize);
        } else if (configValue > 0) {
            rollingSize = configValue;
        } else {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer or the AUTO_VALUE (%d); found '%d'.",
                    ATTRIBUTE_SEGMENT_ROLLING_SIZE, AUTO_VALUE, configValue));
        }

        return new SegmentRollingPolicy(rollingSize);
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
