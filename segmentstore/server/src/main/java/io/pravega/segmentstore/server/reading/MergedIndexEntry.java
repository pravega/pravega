/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import io.pravega.segmentstore.server.ContainerMetadata;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * A ReadIndexEntry that points to data that was merged from a different Segment.
 */
class MergedIndexEntry extends CacheIndexEntry {
    /**
     * Gets a value representing the Id of the Segment that was merged.
     */
    @Getter
    private final long sourceSegmentId;
    /**
     * Gets a value representing the offset inside the SourceSegment where this data is located.
     */
    @Getter
    private final long sourceSegmentOffset;

    /**
     * Creates a new instance of the MergedIndexEntry class.
     *
     * @param streamSegmentOffset The StreamSegment offset for this entry.
     * @param sourceSegmentId     The Id of the Segment that was merged.
     * @param sourceEntry         The CacheIndexEntry this is based on.
     * @throws IllegalArgumentException If offset, length or sourceSegmentOffset are negative numbers.
     * @throws IllegalArgumentException If sourceSegmentId is invalid.
     */
    MergedIndexEntry(long streamSegmentOffset, long sourceSegmentId, CacheIndexEntry sourceEntry) {
        super(streamSegmentOffset, (int) sourceEntry.getLength()); // CacheIndexEntry has length less than Int.Max.
        Preconditions.checkArgument(sourceSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "sourceSegmentId");
        Preconditions.checkArgument(sourceEntry.getStreamSegmentOffset() >= 0, "streamSegmentOffset must be a non-negative number.");

        this.sourceSegmentId = sourceSegmentId;
        this.sourceSegmentOffset = sourceEntry.getStreamSegmentOffset();
        setGeneration(sourceEntry.getGeneration());
    }
}
