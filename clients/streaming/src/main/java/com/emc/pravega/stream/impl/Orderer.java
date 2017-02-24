/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.impl.segment.SegmentInputStream;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used to select which event should go next when consuming from multiple segments.
 *
 */
public class Orderer {
    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * Given a list of segments this reader owns, (which contain their positions) returns the one that should
     * be read from next. This is done in a consistent way. i.e. Calling this method with the same readers at
     * the same positions, should yield the same result. (The passed collection is not modified)
     *
     * @param segments The logs to get the next reader for.
     * @return A segment that this reader should read from next.
     */
    SegmentInputStream nextSegment(List<SegmentInputStream> segments) {
        if (segments.isEmpty()) {
            return null;
        }
        int count = counter.incrementAndGet();
        return segments.get(count % segments.size());
    }
}
