/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * Configuration for {@link ChunkedSegmentStorage}.
 */
@AllArgsConstructor
@Builder(toBuilder = true, builderMethodName = "instanceBuilder")
public class ChunkedSegmentStorageConfig {
    public static final Property<Long> MIN_SIZE_LIMIT_FOR_CONCAT = Property.named("concat.size.bytes.min", 0L);
    public static final Property<Long> MAX_SIZE_LIMIT_FOR_CONCAT = Property.named("concat.size.bytes.max", Long.MAX_VALUE);
    public static final Property<Integer> MAX_BUFFER_SIZE_FOR_APPENDS = Property.named("appends.buffer.size.bytes.max", 1024 * 1024);
    public static final Property<Integer> MAX_INDEXED_SEGMENTS = Property.named("readindex.segments.max", 1024);
    public static final Property<Integer> MAX_INDEXED_CHUNKS_PER_SEGMENTS = Property.named("readindex.chunksPerSegment.max", 1024);
    public static final Property<Integer> MAX_INDEXED_CHUNKS = Property.named("readindex.chunks.max", 16 * 1024);
    public static final Property<Boolean> APPENDS_ENABLED = Property.named("appends.enable", true);
    public static final Property<Long> DEFAULT_ROLLOVER_SIZE = Property.named("metadata.rollover.size.bytes.max", SegmentRollingPolicy.MAX_CHUNK_LENGTH);

    /**
     * Default configuration for {@link ChunkedSegmentStorage}.
     */
    public static final ChunkedSegmentStorageConfig DEFAULT_CONFIG = ChunkedSegmentStorageConfig.instanceBuilder()
            .minSizeLimitForConcat(0L)
            .maxSizeLimitForConcat(Long.MAX_VALUE)
            .defaultRollingPolicy(SegmentRollingPolicy.NO_ROLLING)
            .maxBufferSizeForChunkDataTransfer(1024 * 1024)
            .maxIndexedSegments(1024)
            .maxIndexedChunksPerSegment(1024)
            .maxIndexedChunks(16 * 1024)
            .appendEnabled(true)
            .build();

    static final String COMPONENT_CODE = "storage";

    /**
     * Size of chunk in bytes above which it is no longer considered a small object.
     * For small source objects, concat is not used and instead.
     */
    @Getter
    final private long minSizeLimitForConcat;

    /**
     * Size of chunk in bytes above which it is no longer considered for concat.
     */
    @Getter
    final private long maxSizeLimitForConcat;

    /**
     * A SegmentRollingPolicy to apply to every StreamSegment that does not have its own policy defined.
     */
    @Getter
    @NonNull
    final private SegmentRollingPolicy defaultRollingPolicy;

    /**
     * Maximum size for the buffer used while copying of data from one chunk to other.
     */
    @Getter
    final private int maxBufferSizeForChunkDataTransfer;

    /**
     * Max number of indexed segments to keep in read cache.
     */
    @Getter
    final private int maxIndexedSegments;

    /**
     * Max number of indexed chunks to keep per segment in read cache.
     */
    @Getter
    final private int maxIndexedChunksPerSegment;

    /**
     * Max number of indexed chunks to keep in cache.
     */
    @Getter
    final private int maxIndexedChunks;

    /**
     * Whether the append functionality is enabled or disabled.
     */
    @Getter
    final private boolean appendEnabled;

    /**
     * Creates a new instance of the ChunkedSegmentStorageConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    ChunkedSegmentStorageConfig(TypedProperties properties) throws ConfigurationException {
        this.appendEnabled = properties.getBoolean(APPENDS_ENABLED);
        this.maxBufferSizeForChunkDataTransfer = properties.getInt(MAX_BUFFER_SIZE_FOR_APPENDS);
        this.minSizeLimitForConcat = properties.getLong(MIN_SIZE_LIMIT_FOR_CONCAT);
        this.maxSizeLimitForConcat = properties.getLong(MAX_SIZE_LIMIT_FOR_CONCAT);
        this.maxIndexedSegments = properties.getInt(MAX_INDEXED_SEGMENTS);
        this.maxIndexedChunksPerSegment = properties.getInt(MAX_INDEXED_CHUNKS_PER_SEGMENTS);
        this.maxIndexedChunks = properties.getInt(MAX_INDEXED_CHUNKS);
        long defaultMaxLength = properties.getLong(DEFAULT_ROLLOVER_SIZE);
        this.defaultRollingPolicy = new SegmentRollingPolicy(defaultMaxLength);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ChunkedSegmentStorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ChunkedSegmentStorageConfig::new);
    }
}
