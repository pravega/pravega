/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.stream.Segment;

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
     * @param config  The SegmentInputConfiguration to use.
     * @return New instance of SegmentInputStream for reading.
     */
    SegmentInputStream createInputStreamForSegment(Segment segment, SegmentInputConfiguration config);

    /**
     * Opens an existing segment for reading. This operation will fail if the
     * segment does not exist.
     * This operation may be called multiple times on the same stream from the
     * same client (i.e., there can be concurrent Stream Readers in the same
     * process space).
     * This operation additionally takes a buffer size parameter. This size is
     * used to allocate buffer space for the bytes this stream reads from the
     * segment. It is important to control the buffer size, e.g., when randomly
     * reading events with {@link EventReader#read()}
     *
     * @param segment  The segment to create an input for.
     * @param config   The SegmentInputConfiguration to use.
     * @param bufferSize Size of the input stream read buffer.
     * @return A segment input stream.
     */
    SegmentInputStream createInputStreamForSegment(Segment segment, SegmentInputConfiguration config, int bufferSize);
}
