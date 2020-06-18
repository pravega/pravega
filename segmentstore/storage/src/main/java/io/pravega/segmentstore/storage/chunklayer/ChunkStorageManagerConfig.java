/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * Configuration for {@link ChunkStorageManager}.
 */
@AllArgsConstructor
@Builder(toBuilder = true)
public class ChunkStorageManagerConfig {
    /**
     * Default configuration for {@link ChunkStorageManager}.
     */
    public static final ChunkStorageManagerConfig DEFAULT_CONFIG = ChunkStorageManagerConfig.builder()
            .minSizeLimitForConcat(0)
            .maxSizeLimitForConcat(Long.MAX_VALUE)
            .defaultRollingPolicy(SegmentRollingPolicy.NO_ROLLING)
            .maxBufferSizeForChunkDataTransfer(1024 * 1024)
            .maxIndexedSegments(1024)
            .maxIndexedChunksPerSegment(1024)
            .maxIndexedChunks(16 * 1024)
            .appendsDisabled(false)
            .build();

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
     * Whether the append functionality is disabled.
     */
    @Getter
    final private boolean appendsDisabled;
}
