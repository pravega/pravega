/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventStreamReader;
import java.util.concurrent.Semaphore;

/**
 * Creates {@link SegmentInputStream} for reading from existing segments.
 */
public interface SegmentInputStreamFactory {
    
    /**
     * Opens an existing segment for reading bytes. This operation will fail if the
     * segment does not exist.
     * This operation may be called multiple times on the same stream from the
     * same client (i.e., there can be concurrent Stream Readers in the same
     * process space).
     *
     * @param segment The segment to create an input for.
     * @param tokenProvider The {@link DelegationTokenProvider} instance to be used for obtaining a delegation token.
     * @return New instance of SegmentInputStream for reading.
     */
    SegmentInputStream createInputStreamForSegment(Segment segment, DelegationTokenProvider tokenProvider);
    
    /**
     * Opens an existing segment for reading events. This operation will fail if the
     * segment does not exist.
     * This operation may be called multiple times on the same segment from the
     * same client (i.e., there can be concurrent Event Readers in the same
     * process space).
     *
     * @param segment The segment to create an input for.
     * @return New instance of EventSegmentReader for reading.
     */
    EventSegmentReader createEventReaderForSegment(Segment segment);

    /**
     * Open an existing segment for reading up to the provided end offset. This operation will fail if the segment
     * does not exist.
     *
     * @param segment The segment to create an input for.
     * @param bufferSize The size of the buffer to hold for data incoming on this segment.
     * @param hasData A Semaphore that will have `release` called when data is available.
     * @param endOffset The offset up to which the segment can be read.
     * @return New instance of the EventSegmentReader for reading.
     */
    EventSegmentReader createEventReaderForSegment(Segment segment, int bufferSize, Semaphore hasData, long endOffset);

    /**
     * Opens an existing segment for reading. This operation will fail if the
     * segment does not exist.
     * This operation may be called multiple times on the same segment from the
     * same client (i.e., there can be concurrent Event Readers in the same
     * process space).
     * This operation additionally takes a buffer size parameter. This size is
     * used to allocate buffer space for the bytes this reader reads from the
     * segment. It is important to control the buffer size, e.g., when randomly
     * reading events with {@link EventStreamReader#fetchEvent(EventPointer)}
     *
     * @param segment  The segment to create an input for.
     * @param bufferSize Size of the read buffer.
     * @return A segment event reader.
     */
    EventSegmentReader createEventReaderForSegment(Segment segment, int bufferSize);
}
