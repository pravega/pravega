/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream.mock;

import io.pravega.client.stream.Segment;
import io.pravega.client.stream.impl.segment.SegmentInputStream;
import io.pravega.client.stream.impl.segment.SegmentInputStreamFactory;
import io.pravega.client.stream.impl.segment.SegmentOutputStream;
import io.pravega.client.stream.impl.segment.SegmentOutputStreamFactory;

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
    public SegmentOutputStream createOutputStreamForSegment(Segment segment) {
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
