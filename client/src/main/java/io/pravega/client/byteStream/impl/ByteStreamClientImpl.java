package io.pravega.client.byteStream.impl;

import io.pravega.client.byteStream.ByteStreamClient;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.EventWriterConfig;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ByteStreamClientImpl implements ByteStreamClient {

    private final String scope;
    private final SegmentInputStreamFactory inputStreamFactory;
    private final SegmentOutputStreamFactory outputStreamFactory;
    
    @Override
    public ByteStreamReader createByteStreamReaders(String streamName) {
        return createByteStreamReaders(new Segment(scope, streamName, 0));
    }
    
    ByteStreamReader createByteStreamReaders(Segment segment) {
        return new ByteStreamReaderImpl(inputStreamFactory.createInputStreamForSegment(segment));
    }

    @Override
    public ByteStreamWriter createByteStreamWriter(String streamName) {
        return createByteStreamWriter(new Segment(scope, streamName, 0), EventWriterConfig.builder().build());
    }
    
    ByteStreamWriter createByteStreamWriter(Segment segment, EventWriterConfig config) {
        return new ByteStreamWriterImpl(outputStreamFactory.createOutputStreamForSegment(segment, config));
    }

}
