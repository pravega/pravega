/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl.segment;

import io.pravega.stream.EventPointer;
import io.pravega.stream.Segment;
import io.pravega.stream.EventStreamReader;

/**
 * Creates {@link SegmentInputStream} for reading from existing segments.
 */
public interface SegmentInputStreamFactory {
    /**
     * Opens an existing segment for reading. This operation will fail if the
     * segment does not exist.
     * This operation may be called multiple times on the same stream from the
     * same client (i.e., there can be concurrent Stream Readers in the same
     * process space).
     *
     * @param segment The segment to create an input for.
     * @return New instance of SegmentInputStream for reading.
     */
    SegmentInputStream createInputStreamForSegment(Segment segment);

    /**
     * Opens an existing segment for reading. This operation will fail if the
     * segment does not exist.
     * This operation may be called multiple times on the same stream from the
     * same client (i.e., there can be concurrent Stream Readers in the same
     * process space).
     * This operation additionally takes a buffer size parameter. This size is
     * used to allocate buffer space for the bytes this stream reads from the
     * segment. It is important to control the buffer size, e.g., when randomly
     * reading events with {@link EventStreamReader#read(EventPointer)}
     *
     * @param segment  The segment to create an input for.
     * @param bufferSize Size of the input stream read buffer.
     * @return A segment input stream.
     */
    SegmentInputStream createInputStreamForSegment(Segment segment, int bufferSize);
}
