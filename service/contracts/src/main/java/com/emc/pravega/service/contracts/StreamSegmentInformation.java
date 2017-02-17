/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.contracts;

import com.emc.pravega.common.util.ImmutableDate;

import lombok.Data;

/**
 * General Stream Segment Information.
 */
@Data
public class StreamSegmentInformation implements SegmentProperties {

    private final String name;
    private final long length;
    private final boolean sealed;
    private final boolean deleted;
    private final ImmutableDate lastModified;

    /**
     * Creates a new instance of the StreamSegmentInformation class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param length            The length of the StreamSegment.
     * @param isSealed          Whether the StreamSegment is sealed (for modifications).
     * @param isDeleted         Whether the StreamSegment is deleted (does not exist).
     * @param lastModified      The last time the StreamSegment was modified.
     */
    public StreamSegmentInformation(String streamSegmentName, long length, boolean isSealed, boolean isDeleted, ImmutableDate lastModified) {
        this.name = streamSegmentName;
        this.length = length;
        this.sealed = isSealed;
        this.deleted = isDeleted;
        this.lastModified = lastModified;
    }
    
    @Override
    public String toString() {
        return String.format("Name = %s, Length = %d, Sealed = %s, Deleted = %s, LastModified = %s", getName(), getLength(), isSealed(), isDeleted(), getLastModified());
    }

}
