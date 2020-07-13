/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.byteStream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentOutputStreamFactoryImpl;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.common.concurrent.Futures;
import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
public class ByteStreamClientImpl implements ByteStreamClientFactory {
    @NonNull
    private final String scope;
    @NonNull
    private final Controller controller;
    @NonNull
    private final ConnectionFactory connectionFactory;
    @NonNull
    private final SegmentInputStreamFactory inputStreamFactory;
    @NonNull
    private final SegmentOutputStreamFactory outputStreamFactory;
    @NonNull
    private final SegmentMetadataClientFactory metaStreamFactory;
    
    public ByteStreamClientImpl(String scope, Controller controller, ConnectionFactory connectionFactory) {
        this.scope = Preconditions.checkNotNull(scope);
        this.controller = Preconditions.checkNotNull(controller);
        this.connectionFactory = Preconditions.checkNotNull(connectionFactory);
        this.inputStreamFactory = new SegmentInputStreamFactoryImpl(controller, connectionFactory);
        this.outputStreamFactory = new SegmentOutputStreamFactoryImpl(controller, connectionFactory);
        this.metaStreamFactory = new SegmentMetadataClientFactoryImpl(controller, connectionFactory);
    }
    
    @Override
    public ByteStreamReader createByteStreamReader(String streamName) {
        return createByteStreamReaders(new Segment(scope, streamName, 0));
    }

    private ByteStreamReader createByteStreamReaders(Segment segment) {
        String delegationToken = Futures.getAndHandleExceptions(controller.getOrRefreshDelegationTokenFor(segment.getScope(),
                                                                                                          segment.getStream()
                                                                                                                 .getStreamName()),
                                                                RuntimeException::new);

        DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.create(delegationToken, controller, segment);
        SegmentMetadataClient metaClient = metaStreamFactory.createSegmentMetadataClient(segment, tokenProvider);
        long startOffset = metaClient.getSegmentInfo().getStartingOffset();
        return new ByteStreamReaderImpl(inputStreamFactory.createInputStreamForSegment(segment, tokenProvider, startOffset),
                metaClient);
    }

    @Override
    public ByteStreamWriter createByteStreamWriter(String streamName) {
        StreamSegments segments = Futures.getThrowingException(controller.getCurrentSegments(scope, streamName));
        Preconditions.checkState(segments.getNumberOfSegments() > 0, "Stream is sealed");
        Preconditions.checkState(segments.getNumberOfSegments() == 1, "Stream is configured with more than one segment");
        Segment segment = segments.getSegments().iterator().next();
        EventWriterConfig config = EventWriterConfig.builder().build();
        String delegationToken = segments.getDelegationToken();
        DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.create(delegationToken, controller, segment);
        return new BufferedByteStreamWriterImpl(
                new ByteStreamWriterImpl(outputStreamFactory.createOutputStreamForSegment(segment, config, tokenProvider),
                metaStreamFactory.createSegmentMetadataClient(segment, tokenProvider)));
    }

    @Override
    public void close() {
        controller.close();
        connectionFactory.close();
    }
}
