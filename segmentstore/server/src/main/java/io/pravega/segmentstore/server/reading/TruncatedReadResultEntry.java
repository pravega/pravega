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

import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;

/**
 * ReadResultEntry for data that is no longer available due to the StreamSegment being truncated beyond its starting offset.
 */
class TruncatedReadResultEntry extends ReadResultEntryBase {
    /**
     * Creates a new instance of the ReadResultEntry class.
     *
     * @param segmentReadOffset   The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     * @param segmentStartOffset  The first offset in the StreamSegment available for reading.
     */
    TruncatedReadResultEntry(long segmentReadOffset, int requestedReadLength, long segmentStartOffset) {
        super(ReadResultEntryType.Truncated, segmentReadOffset, requestedReadLength);
        fail(new StreamSegmentTruncatedException(segmentStartOffset));
    }
}
