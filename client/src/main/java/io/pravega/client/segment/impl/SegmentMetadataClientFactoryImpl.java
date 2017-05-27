package io.pravega.client.segment.impl;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.impl.Controller;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SegmentMetadataClientFactoryImpl implements SegmentMetadataClientFactory {

    private final Controller controller;
    private final ConnectionFactory cf;
    
    @Override
    public SegmentMetadataClient createSegmentMetadataClient(Segment segment) {
        return new SegmentMetadataClientImpl(segment, controller, cf);
    }

}
