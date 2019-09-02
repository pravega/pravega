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
