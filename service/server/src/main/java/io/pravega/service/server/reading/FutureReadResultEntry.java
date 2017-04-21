/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.reading;

import io.pravega.service.contracts.ReadResultEntryType;

/**
 * Read Result Entry for data that is not yet available in the StreamSegment (for an offset that is beyond the
 * StreamSegment's DurableLogLength)
 */
class FutureReadResultEntry extends ReadResultEntryBase {
    /**
     * Creates a new instance of the FutureReadResultEntry class.
     *
     * @param streamSegmentOffset The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     * @throws IllegalArgumentException If type is not ReadResultEntryType.Future or ReadResultEntryType.Storage.
     */
    FutureReadResultEntry(long streamSegmentOffset, int requestedReadLength) {
        super(ReadResultEntryType.Future, streamSegmentOffset, requestedReadLength);
    }
}
