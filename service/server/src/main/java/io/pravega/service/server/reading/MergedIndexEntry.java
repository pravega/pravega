/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.reading;

import io.pravega.service.server.ContainerMetadata;
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
