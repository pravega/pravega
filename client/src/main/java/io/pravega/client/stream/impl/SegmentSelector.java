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

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.Stream;
import io.pravega.common.concurrent.FutureHelpers;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

/**
 * A class that determines to which segment an event associated with a routing key will go. This is
 * invoked on every writeEvent call to decide how to send a particular segment. It is acceptable for
 * it to cache the current set of segments for a stream, as it will be queried again if a segment
 * has been sealed.
 */
@Slf4j
@RequiredArgsConstructor
public class SegmentSelector {

    private final Stream stream;
    private final Controller controller;
    private final SegmentOutputStreamFactory outputStreamFactory;
    @GuardedBy("$lock")
    private final Random random = new Random();
    @GuardedBy("$lock")
    private StreamSegments currentSegments;
    @GuardedBy("$lock")
    private final Map<Segment, SegmentOutputStream> writers = new HashMap<>();

    /**
     * Selects which segment an event should be written to.
     *
     * @param routingKey The key that should be used to select from the segment that the event
     *            should go to.
     * @return The SegmentOutputStream for the segment that has been selected or null if
     *         {@link #refreshSegmentEventWriters()} needs to be called.
     */
    @Synchronized
    public SegmentOutputStream getSegmentOutputStreamForKey(String routingKey) {
        if (currentSegments == null) {
            return null;
        }
        return writers.get(getSegmentForEvent(routingKey));
    }

    @Synchronized
    public Segment getSegmentForEvent(String routingKey) {
        if (currentSegments == null) {
            return null;
        }
        if (routingKey == null) {
            return currentSegments.getSegmentForKey(random.nextDouble());
        }
        return currentSegments.getSegmentForKey(routingKey);
    }

    public List<PendingEvent> refreshSegmentEventWritersUponSealed(Segment sealedSegment) {
        StreamSegmentsWithPredecessors successors = FutureHelpers.getAndHandleExceptions(
                controller.getSuccessors(sealedSegment), RuntimeException::new);
        return updateSegments(currentSegments.withReplacementRange(successors));
    }

    /**
     * Refresh the latest list of segments in the given stream.
     * 
     * @return A list of events that were sent to old segments and never acked. These should be
     *         re-sent.
     */
    public List<PendingEvent> refreshSegmentEventWriters() {
        return updateSegments(FutureHelpers.getAndHandleExceptions(
                controller.getCurrentSegments(stream.getScope(), stream.getStreamName()), RuntimeException::new));
    }

    @Synchronized
    private List<PendingEvent> updateSegments(StreamSegments newSteamSegments) {
        currentSegments = newSteamSegments;
        for (Segment segment : currentSegments.getSegments()) {
            if (!writers.containsKey(segment)) {
                SegmentOutputStream out = outputStreamFactory.createOutputStreamForSegment(segment);
                writers.put(segment, out);
            }
        }
        List<PendingEvent> toResend = new ArrayList<>();
        Iterator<Entry<Segment, SegmentOutputStream>> iter = writers.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Segment, SegmentOutputStream> entry = iter.next();
            if (!currentSegments.getSegments().contains(entry.getKey())) {
                SegmentOutputStream writer = entry.getValue();
                iter.remove();
                try {
                    writer.close();
                } catch (SegmentSealedException e) {
                    log.info("Caught segment sealed while refreshing on segment {}", entry.getKey());
                }
                toResend.addAll(writer.getUnackedEvents());
            }
        }
        return toResend;
    }

    @Synchronized
    public List<Segment> getSegments() {
        if (currentSegments == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(currentSegments.getSegments());
    }

    @Synchronized
    public List<SegmentOutputStream> getWriters() {
        return new ArrayList<>(writers.values());
    }

}
