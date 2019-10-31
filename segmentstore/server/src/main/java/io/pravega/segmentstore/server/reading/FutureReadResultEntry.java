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

/**
 * Read Result Entry for data that is not yet available in the StreamSegment (for an offset that is beyond the
 * StreamSegment's Length)
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
