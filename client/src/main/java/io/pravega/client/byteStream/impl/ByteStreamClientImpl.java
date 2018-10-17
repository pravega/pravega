/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
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
    public ByteStreamReader createByteStreamReader(String streamName) {
        return createByteStreamReaders(new Segment(scope, streamName, 0));
    }

    ByteStreamReader createByteStreamReaders(Segment segment) {
        String delegationToken = Futures.getAndHandleExceptions(controller.getOrRefreshDelegationTokenFor(segment.getScope(),
                                                                                                          segment.getStream()
                                                                                                                 .getStreamName()),
                                                                RuntimeException::new);
        return new ByteStreamReaderImpl(inputStreamFactory.createInputStreamForSegment(segment, delegationToken),
                                        metaStreamFactory.createSegmentMetadataClient(segment, delegationToken));
    }

    @Override
    public ByteStreamWriter createByteStreamWriter(String streamName) {
        return createByteStreamWriter(new Segment(scope, streamName, 0), EventWriterConfig.builder().build());
    }

    ByteStreamWriter createByteStreamWriter(Segment segment, EventWriterConfig config) {
        String delegationToken = Futures.getAndHandleExceptions(controller.getOrRefreshDelegationTokenFor(segment.getScope(),
                                                                                                          segment.getStreamName()),
                                                                RuntimeException::new);
        return new BufferedByteStreamWriterImpl(new ByteStreamWriterImpl(outputStreamFactory.createOutputStreamForSegment(segment,
                                                                                                                          config,
                                                                                                                          delegationToken),
                                                                         metaStreamFactory.createSegmentMetadataClient(segment,
                                                                                                                       delegationToken)));
    }

}
