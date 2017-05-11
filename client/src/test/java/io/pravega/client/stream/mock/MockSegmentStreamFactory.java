/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.mock;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
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
    public SegmentOutputStream createOutputStreamForSegment(UUID writerId, Segment segment) {
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
