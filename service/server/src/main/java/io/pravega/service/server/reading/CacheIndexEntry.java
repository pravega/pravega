/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.reading;

import com.google.common.base.Preconditions;

/**
 * Represents a ReadIndexEntry that points to an entry in the Cache.
 */
public class CacheIndexEntry extends ReadIndexEntry {
    private final int length;

    /**
     * Creates a new instance of the ReadIndexEntry class.
     *
     * @param streamSegmentOffset The StreamSegment offset for this entry.
     * @param length              The Length of this entry.
     * @throws IllegalArgumentException if the offset is a negative number.
     * @throws IllegalArgumentException if the length is a negative number.
     */
    CacheIndexEntry(long streamSegmentOffset, int length) {
        super(streamSegmentOffset);
        Preconditions.checkArgument(length >= 0, "length", "length must be a non-negative number.");
        this.length = length;
    }

    @Override
    long getLength() {
        return this.length;
    }

    @Override
    boolean isDataEntry() {
        return true;
    }
}
