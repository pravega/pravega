/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
 * Read Index Entry that redirects to a different StreamSegment.
 */
class RedirectIndexEntry extends ReadIndexEntry {
    private final StreamSegmentReadIndex redirectReadIndex;

    /**
     * Creates a new instance of the RedirectIndexEntry class.
     *
     * @param streamSegmentOffset The StreamSegment offset for this entry.
     * @param redirectReadIndex   The StreamSegmentReadIndex to redirect to.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException if the offset is a negative number.
     */
    RedirectIndexEntry(long streamSegmentOffset, StreamSegmentReadIndex redirectReadIndex) {
        super(streamSegmentOffset);

        Preconditions.checkNotNull(redirectReadIndex, "redirectReadIndex");
        this.redirectReadIndex = redirectReadIndex;
    }

    /**
     * Gets a reference to the StreamSegmentReadIndex that this entry redirects to.
     */
    StreamSegmentReadIndex getRedirectReadIndex() {
        return this.redirectReadIndex;
    }

    @Override
    public long getLength() {
        return this.redirectReadIndex.getSegmentLength();
    }

    @Override
    boolean isDataEntry() {
        return false;
    }

    @Override
    int getCacheAddress() {
        throw new UnsupportedOperationException();
    }
}
