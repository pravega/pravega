/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.common.MathHelpers;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Used to select which event should go next when consuming from multiple segments.
 *
 */
@RequiredArgsConstructor
@Slf4j
public class Orderer {
    private final AtomicInteger counter = new AtomicInteger(0);

    @VisibleForTesting
    Orderer(int initialCount) {
        this();
        counter.set(initialCount);
    }
    
    /**
     * Given a list of segments this reader owns, (which contain their positions) returns the one
     * that should be read from next. This is done in way to minimize blocking and ensure fairness.
     * 
     * This is done by calling {@link SegmentInputStream#isSegmentReady()} on each segment.
     * This method will prefer to return streams where that method is true. This method should
     * reflect that the next call to {@link SegmentInputStream#read()} will not block (either
     * because it has data or will throw {@link EndOfSegmentException}
     *
     * @param <T> The type of the SegmentInputStream that is being selected from.
     * @param segments The logs to get the next reader for.
     * @return A segment that this reader should read from next.
     */
    @VisibleForTesting
    public <T extends SegmentInputStream> T nextSegment(List<T> segments) {
        if (segments.isEmpty()) {
            return null;
        }
        for (int i = 0; i < segments.size(); i++) {
            T inputStream = segments.get(MathHelpers.abs(counter.incrementAndGet()) % segments.size());
            if (inputStream.bytesInBuffer() != 0) {
                log.trace("Selecting segment: " + inputStream.getSegmentId());
                return inputStream;
            } else {
                inputStream.fillBuffer();
            }
        }
        return segments.get(MathHelpers.abs(counter.incrementAndGet()) % segments.size());
    }
}
