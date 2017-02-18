/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.hash.HashHelper;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicReference;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import lombok.RequiredArgsConstructor;

/**
 * A class that determines to which segment an event associated with a routing key will go.
 * This is invoked on every writeEvent call to decide how to send a particular segment.
 * It is acceptable for it to cache the current set of segments for a stream, as it will be queried again
 * if a segment has been sealed.
 */
@RequiredArgsConstructor
public class EventRouter {

    private static final HashHelper HASHER = HashHelper.seededWith("EventRouter");

    private final Stream stream;
    private final Controller controller;
    private final AtomicReference<StreamSegments> currentSegments = new AtomicReference<>();

    /**
     * Selects which segment an event should be written to.
     *
     * @param routingKey The key that should be used to select from the segment that the event should go to.
     * @return The Segment that has been selected.
     */
    public Segment getSegmentForEvent(String routingKey) {
        Preconditions.checkNotNull(routingKey);
        StreamSegments streamSegments = currentSegments.get();
        if (streamSegments == null) {
            refreshSegmentList();
            streamSegments = currentSegments.get();
        }
        return streamSegments.getSegmentForKey(HASHER.hashToRange(routingKey));
    }

    /**
     * Refresh the latest list of segments in the given stream.
     */
    public void refreshSegmentList() {
        currentSegments.set(getAndHandleExceptions(controller.getCurrentSegments(stream.getScope(), stream.getStreamName()), RuntimeException::new));
    }

}
