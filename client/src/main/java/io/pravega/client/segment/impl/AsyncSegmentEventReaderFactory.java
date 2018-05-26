/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventStreamReader;

/**
 * Creates {@link AsyncSegmentEventReader} for reading events from existing segments.
 */
public interface AsyncSegmentEventReaderFactory {

    int DEFAULT_BUFFER_SIZE = AsyncSegmentEventReaderImpl.DEFAULT_READ_LENGTH;

    /**
     * Opens an existing segment for reading. This operation will fail if the
     * segment does not exist.
     * This operation may be called multiple times on the same stream from the
     * same client (i.e., there can be concurrent Stream Readers in the same
     * process space).
     * This operation additionally takes a buffer size parameter. This value determines the prefetch size,
     * i.e., the maximum number of bytes read in each network call. It is important to control the buffer size,
     * e.g., when randomly reading events with {@link EventStreamReader#fetchEvent(EventPointer)}
     *
     * @param segment  The segment to create a reader for.
     * @param bufferSize Size of the read buffer.
     * @return A segment input stream.
     */
    default AsyncSegmentEventReader createEventReaderForSegment(Segment segment, int bufferSize) {
        return createEventReaderForSegment(segment, AsyncSegmentEventReaderImpl.UNBOUNDED_END_OFFSET, bufferSize);
    }

    /**
     * Opens an existing segment for reading up to the given end offset (exclusive). This operation will fail if the
     * segment does not exist.
     * This operation may be called multiple times on the same stream from the
     * same client (i.e., there can be concurrent Stream Readers in the same
     * process space).
     * This operation additionally takes a buffer size parameter. This value determines the prefetch size,
     * i.e., the maximum number of bytes read in each network call. It is important to control the buffer size,
     * e.g., when randomly reading events with {@link EventStreamReader#fetchEvent(EventPointer)}
     *
     * @param segment  The segment to create a reader for.
     * @param endOffset The offset up to which the segment can be read.
     * @param bufferSize Size of the read buffer.
     * @return A segment input stream.
     */
    AsyncSegmentEventReader createEventReaderForSegment(Segment segment, long endOffset, int bufferSize);
}
