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

import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Stream;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.util.RetriesExhaustedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.function.Consumer;
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
    private final Random random = RandomFactory.create();
    @GuardedBy("$lock")
    private StreamSegments currentSegments;
    @GuardedBy("$lock")
    private final Map<Segment, SegmentOutputStream> writers = new HashMap<>();
    private final EventWriterConfig config;

    /**
     * Selects which segment an event should be written to.
     *
     * @param routingKey The key that should be used to select from the segment that the event
     *            should go to.
     * @return The SegmentOutputStream for the segment that has been selected or null if
     *         {@link #refreshSegmentEventWriters(Consumer)} needs to be called.
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

    public List<PendingEvent> refreshSegmentEventWritersUponSealed(Segment sealedSegment, Consumer<Segment> segmentSealedCallback) {
        StreamSegmentsWithPredecessors successors = Futures.getAndHandleExceptions(
                controller.getSuccessors(sealedSegment), t -> {
                    log.error("Error while fetching successors for segment: {}", sealedSegment, t);
                    // Remove all writers and fail all pending writes
                    Exception e = (t instanceof RetriesExhaustedException) ? new ControllerFailureException(t) : new NoSuchSegmentException(sealedSegment.toString());
                    removeAllWriters().forEach(event -> event.getAckFuture().completeExceptionally(e));
                    return null;
                });

        if (successors == null) {
            return Collections.emptyList();
        } else {
            return updateSegmentsUponSealed(successors, sealedSegment, segmentSealedCallback);
        }
    }

    /**
     * Refresh the latest list of segments in the given stream.
     *
     * @param segmentSealedCallBack Method to be executed on receiving SegmentSealed from SSS.
     * @return A list of events that were sent to old segments and never acked. These should be
     *         re-sent.
     */
    public List<PendingEvent> refreshSegmentEventWriters(Consumer<Segment> segmentSealedCallBack) {
        log.info("Refreshing segments for stream {}", stream);
        return updateSegments(Futures.getAndHandleExceptions(
                controller.getCurrentSegments(stream.getScope(), stream.getStreamName()), RuntimeException::new),
                segmentSealedCallBack);
    }

    /**
     * Remove a segment writer.
     * @param segment The segment whose writer should be removed.
     */
    @Synchronized
    void removeSegmentWriter(Segment segment) {
        writers.remove(segment);
    }

    @Synchronized
    private List<PendingEvent> updateSegments(StreamSegments newSteamSegments, Consumer<Segment>
            segmentSealedCallBack) {
        currentSegments = newSteamSegments;
        createMissingWriters(segmentSealedCallBack, newSteamSegments.getDelegationToken());
        List<PendingEvent> toResend = new ArrayList<>();
        Iterator<Entry<Segment, SegmentOutputStream>> iter = writers.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Segment, SegmentOutputStream> entry = iter.next();
            if (!currentSegments.getSegments().contains(entry.getKey())) {
                SegmentOutputStream writer = entry.getValue();
                log.info("Closing writer {} on segment {} during segment refresh", writer, entry.getKey());
                iter.remove();
                try {
                    writer.close();
                } catch (SegmentSealedException e) {
                    log.info("Caught segment sealed while refreshing on segment {}", entry.getKey());
                }
                toResend.addAll(writer.getUnackedEventsOnSeal());
            }
        }
        return toResend;
    }

    @Synchronized
    private List<PendingEvent> updateSegmentsUponSealed(StreamSegmentsWithPredecessors successors, Segment sealedSegment,
                                                        Consumer<Segment> segmentSealedCallback) {
        currentSegments = currentSegments.withReplacementRange(sealedSegment, successors);
        createMissingWriters(segmentSealedCallback, currentSegments.getDelegationToken());
        log.debug("Fetch unacked events for segment: {}, and adding new segments {}", sealedSegment, currentSegments);
        return writers.get(sealedSegment).getUnackedEventsOnSeal();
    }

    @Synchronized
    private List<PendingEvent> removeAllWriters() {
        //get all pending events.
        List<PendingEvent> pendingEvents = new ArrayList<>();
        writers.values().forEach(out -> {
            pendingEvents.addAll(out.getUnackedEventsOnSeal());
        });
        // remove all writers.
        writers.clear();
        return pendingEvents;
    }

    private void createMissingWriters(Consumer<Segment> segmentSealedCallBack, String delegationToken) {
        for (Segment segment : currentSegments.getSegments()) {
            if (!writers.containsKey(segment)) {
                log.debug("Creating writer for segment {}", segment);
                SegmentOutputStream out = outputStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallBack, config, delegationToken);
                writers.put(segment, out);
            }
        }
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
