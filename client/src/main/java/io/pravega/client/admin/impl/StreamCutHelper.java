/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.admin.impl;

import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInfo;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class to obtain the current HEAD and TAIL {@link StreamCut}s for a given {@link Stream}.
 */
@Slf4j
public class StreamCutHelper {
    private final Controller controller;
    private final SegmentMetadataClientFactory segmentMetadataClientFactory;

    public StreamCutHelper(Controller controller, ConnectionPool connectionPool) {
        this.controller = controller;
        this.segmentMetadataClientFactory = new SegmentMetadataClientFactoryImpl(controller, connectionPool);
    }

    /**
     * Obtain the {@link StreamCut} pointing to the current HEAD of the Stream.
     * @param stream The Stream.
     * @return {@link StreamCut} pointing to the HEAD of the Stream.
     */
    public CompletableFuture<StreamCut> fetchHeadStreamCut(final Stream stream) {
        //Fetch segments pointing to the current HEAD of the stream.
        return controller.getSegmentsAtTime(new StreamImpl(stream.getScope(), stream.getStreamName()), 0L)
                         .thenApply( s -> new StreamCutImpl(stream, s));
    }

    /**
     * Obtain the {@link StreamCut} pointing to the current TAIL of the Stream.
     * @param stream The Stream.
     * @return {@link StreamCut} pointing to the TAIL of the Stream.
     */
    public CompletableFuture<StreamCut> fetchTailStreamCut(final Stream stream) {
        final DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.create(controller,
                stream.getScope(), stream.getStreamName());
        return controller.getCurrentSegments(stream.getScope(), stream.getStreamName())
                         .thenApply(streamsegments -> {
                             tokenProvider.populateToken(streamsegments.getDelegationToken());
                             Map<Segment, Long> pos = streamsegments.getSegments().stream()
                                             .map(segment -> segmentToInfo(segment, tokenProvider))
                                             .collect(Collectors.toMap(SegmentInfo::getSegment, SegmentInfo::getWriteOffset));
                             return new StreamCutImpl(stream, pos);
                         });
    }

    private SegmentInfo segmentToInfo(Segment s, DelegationTokenProvider tokenProvider) {
        @Cleanup
        SegmentMetadataClient client = segmentMetadataClientFactory.createSegmentMetadataClient(s, tokenProvider);
        return client.getSegmentInfo();
    }
}
