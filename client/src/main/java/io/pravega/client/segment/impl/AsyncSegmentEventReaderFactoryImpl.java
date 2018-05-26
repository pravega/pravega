/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;

@Slf4j
@VisibleForTesting
@RequiredArgsConstructor
public class AsyncSegmentEventReaderFactoryImpl implements AsyncSegmentEventReaderFactory {

    private final Controller controller;
    private final ConnectionFactory cf;

    @Override
    public AsyncSegmentEventReader createEventReaderForSegment(Segment segment, long endOffset, int bufferSize) {
        String delegationToken = Futures.getAndHandleExceptions(controller.getOrRefreshDelegationTokenFor(segment.getScope(), segment.getStream().getStreamName()), RuntimeException::new);
        AsyncSegmentInputStreamImpl result = new AsyncSegmentInputStreamImpl(controller, cf, segment, delegationToken);
        try {
            Exceptions.handleInterrupted(() -> result.getConnection().get());
        } catch (ExecutionException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return new AsyncSegmentEventReaderImpl(result, 0, endOffset,  bufferSize);
    }
}
