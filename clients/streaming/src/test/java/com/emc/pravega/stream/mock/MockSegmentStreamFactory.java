/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.mock;

import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentInputStreamFactory;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactory;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class MockSegmentStreamFactory implements SegmentInputStreamFactory, SegmentOutputStreamFactory {

    private final Map<Segment, MockSegmentIoStreams> segments = new ConcurrentHashMap<>();

    @Override
    public SegmentOutputStream createOutputStreamForTransaction(Segment segment, UUID txId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SegmentOutputStream createOutputStreamForSegment(Segment segment)
            throws SegmentSealedException {
        MockSegmentIoStreams streams = new MockSegmentIoStreams(segment);
        segments.putIfAbsent(segment, streams);
        return segments.get(segment);
    }

    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment, int bufferSize) {
        return createInputStreamForSegment(segment);
    }

    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment) {
        MockSegmentIoStreams streams = new MockSegmentIoStreams(segment);
        segments.putIfAbsent(segment, streams);
        return segments.get(segment);
    }
}
