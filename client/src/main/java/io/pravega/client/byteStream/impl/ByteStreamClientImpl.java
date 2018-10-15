package io.pravega.client.byteStream.impl;

import io.pravega.client.byteStream.ByteStreamClient;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.concurrent.Futures;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ByteStreamClientImpl implements ByteStreamClient {

    private final String scope;
    private final Controller controller;
    private final SegmentInputStreamFactory inputStreamFactory;
    private final SegmentOutputStreamFactory outputStreamFactory;
    private final SegmentMetadataClientFactory metaStreamFactory;
    
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
        String delegationToken = Futures.getAndHandleExceptions(controller.getOrRefreshDelegationTokenFor(segment.getScope(),
                                                                                                          segment.getStreamName()),
                                                                RuntimeException::new);
        return new ByteStreamWriterImpl(outputStreamFactory.createOutputStreamForSegment(segment, config,
                                                                                         delegationToken),
                                        metaStreamFactory.createSegmentMetadataClient(segment, delegationToken));
    }

}
