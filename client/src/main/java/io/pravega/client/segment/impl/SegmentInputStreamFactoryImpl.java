/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.MathHelpers;
import io.pravega.common.concurrent.Futures;
import lombok.RequiredArgsConstructor;

@VisibleForTesting
@RequiredArgsConstructor
public class SegmentInputStreamFactoryImpl implements SegmentInputStreamFactory {

    private final Controller controller;
    private final ConnectionFactory cf;

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment) {
        return createEventReaderForSegment(segment, SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);
    }

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment, long endOffset) {
        return getEventSegmentReader(segment, endOffset, SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);
    }

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment, int bufferSize) {
        return getEventSegmentReader(segment, Long.MAX_VALUE, bufferSize);
    }

    private EventSegmentReader getEventSegmentReader(Segment segment, long endOffset, int bufferSize) {
        String delegationToken = Futures.getAndHandleExceptions(controller.getOrRefreshDelegationTokenFor(segment.getScope(),
                                                                                                          segment.getStream()
                                                                                                                 .getStreamName()),
                                                                RuntimeException::new);
        AsyncSegmentInputStreamImpl async = new AsyncSegmentInputStreamImpl(controller, cf, segment,
                DelegationTokenProviderFactory.create(delegationToken, controller, segment));
        async.getConnection();                      //Sanity enforcement
        bufferSize = MathHelpers.minMax(bufferSize, SegmentInputStreamImpl.MIN_BUFFER_SIZE, SegmentInputStreamImpl.MAX_BUFFER_SIZE);
        return getEventSegmentReader(async, 0, endOffset, bufferSize);
    }

    @VisibleForTesting
    static EventSegmentReaderImpl getEventSegmentReader(AsyncSegmentInputStream async, long startOffset,
                                                                  long endOffset, int bufferSize) {
        return new EventSegmentReaderImpl(new SegmentInputStreamImpl(async, startOffset, endOffset, bufferSize));
    }

    @VisibleForTesting
    static EventSegmentReaderImpl getEventSegmentReader(AsyncSegmentInputStream async, long startOffset) {
        return new EventSegmentReaderImpl(new SegmentInputStreamImpl(async, startOffset));
    }

    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment, DelegationTokenProvider tokenProvider) {
        AsyncSegmentInputStreamImpl async = new AsyncSegmentInputStreamImpl(controller, cf, segment, tokenProvider);
        async.getConnection();
        return new SegmentInputStreamImpl(async, 0);
    }
}
