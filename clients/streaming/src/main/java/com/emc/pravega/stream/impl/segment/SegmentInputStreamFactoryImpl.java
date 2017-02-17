/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl.segment;

import java.util.concurrent.ExecutionException;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.google.common.annotations.VisibleForTesting;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@VisibleForTesting
@RequiredArgsConstructor
public class SegmentInputStreamFactoryImpl implements SegmentInputStreamFactory {

    private final Controller controller;
    private final ConnectionFactory cf;
    
    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment, SegmentInputConfiguration config) {
        return createInputStreamForSegment(segment, config, SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);
    }

    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment, SegmentInputConfiguration config, int bufferSize) {
    AsyncSegmentInputStreamImpl result = new AsyncSegmentInputStreamImpl(controller, cf, segment.getScopedName());
        try {
            Exceptions.handleInterrupted(() -> result.getConnection().get());
        } catch (ExecutionException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return new SegmentInputStreamImpl(result, 0, bufferSize);
    }
}
