/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.stream.mock;

import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.segment.impl.ConditionalOutputStream;
import io.pravega.client.segment.impl.ConditionalOutputStreamFactory;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.EventWriterConfig;
import lombok.val;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public class MockSegmentStreamFactory implements SegmentInputStreamFactory, SegmentOutputStreamFactory, ConditionalOutputStreamFactory, SegmentMetadataClientFactory {

    private final Map<Segment, MockSegmentIoStreams> segments = new ConcurrentHashMap<>();

    @Override
    public SegmentOutputStream createOutputStreamForTransaction(Segment segment, UUID txId,
                                                                EventWriterConfig config,
                                                                DelegationTokenProvider tokenProvider) {
        throw new UnsupportedOperationException();
    }

    private MockSegmentIoStreams getMockStream(Segment segment) {
        MockSegmentIoStreams streams = new MockSegmentIoStreams(segment, null);
        segments.putIfAbsent(segment, streams);
        return segments.get(segment);
    }
    
    @Override
    public SegmentOutputStream createOutputStreamForSegment(Segment segment, Consumer<Segment> segmentSealedCallback,
                                                            EventWriterConfig config, DelegationTokenProvider tokenProvider) {
        return getMockStream(segment);
    }

    @Override
    public SegmentOutputStream createOutputStreamForSegment(Segment segment, EventWriterConfig config, DelegationTokenProvider tokenProvider) {
        return getMockStream(segment);
    }

    @Override
    public ConditionalOutputStream createConditionalOutputStream(Segment segment, DelegationTokenProvider tokenProvider, EventWriterConfig config) {
        return getMockStream(segment);
    }

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment, int bufferSize) {
        return createEventReaderForSegment(segment);
    }

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment) {
        return getMockStream(segment);
    }

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment, long startOffset, long endOffset) {
        val reader =  getMockStream(segment);
        reader.setOffset(startOffset);
        return reader;
    }

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment, long startOffset, int bufferSize) {
        val reader = getMockStream(segment);
        reader.setOffset(startOffset);
        return  reader;
    }

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment, int bufferSize, Semaphore hasData, long endOffset) {
        MockSegmentIoStreams streams = new MockSegmentIoStreams(segment, hasData);
        segments.putIfAbsent(segment, streams);
        return segments.get(segment);
    }

    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment, DelegationTokenProvider tokenProvider) {
        return getMockStream(segment);
    }

    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment, DelegationTokenProvider tokenProvider, long startOffset) {
        return getMockStream(segment);
    }

    @Override
    public SegmentMetadataClient createSegmentMetadataClient(Segment segment, DelegationTokenProvider tokenProvider) {
        return getMockStream(segment);
    }
}
