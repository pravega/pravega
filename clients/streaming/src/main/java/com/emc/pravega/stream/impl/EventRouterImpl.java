package com.emc.pravega.stream.impl;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import java.util.concurrent.atomic.AtomicReference;

import com.emc.pravega.stream.EventRouter;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamSegments;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class EventRouterImpl implements EventRouter {

    
    private final Stream stream;
    private final Controller controller;
    private final AtomicReference<StreamSegments> currentSegments = new AtomicReference<>();

    
    @Override
    public Segment getSegmentForEvent(String routingKey) {
        StreamSegments streamSegments = currentSegments.get();
        if (streamSegments == null) {
            refreshSegmentList();
            streamSegments = currentSegments.get();
        }
        long hashCode = Hashing.murmur3_128().hashUnencodedChars(routingKey).asLong();
        return streamSegments.getSegmentForKey(longToDoubleFraction(hashCode));
    }
    
    @Override
    public void refreshSegmentList() {
        currentSegments.set(getAndHandleExceptions(controller.getCurrentSegments(stream.getQualifiedName()), RuntimeException::new));
    }
    
    
    private static final long LEADING_BITS = 0x3ff0000000000000L;
    private static final long MASK =         0x000fffffffffffffL;
    /**
     * Turns the leading 54 bits of a long into a double fraction between 0 and 1.
     */
    @VisibleForTesting
    static double longToDoubleFraction(long value) {
        long shifted = (value >> 12) & MASK;
        return Double.longBitsToDouble(LEADING_BITS + shifted) - 1;
    }
 
}
