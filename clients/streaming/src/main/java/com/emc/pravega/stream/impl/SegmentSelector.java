/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.hash.HashHelper;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactory;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
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

    private static final HashHelper HASHER = HashHelper.seededWith("EventRouter");

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
        return currentSegments.getSegmentForKey(HASHER.hashToRange(routingKey));
    }

    @Synchronized
    public void removeWriter(SegmentOutputStream outputStream) {
        writers.values().remove(outputStream);
    }

    /**
     * Refresh the latest list of segments in the given stream.
     * 
     * @return A list of events that were sent to old segments and never acked. These should be
     *         re-sent.
     */
    @Synchronized
    public List<PendingEvent> refreshSegmentEventWriters() {
        List<PendingEvent> toResend = new ArrayList<>();
        currentSegments = FutureHelpers.getAndHandleExceptions(controller.getCurrentSegments(stream.getScope(),
                                                                                             stream.getStreamName()),
                                                               RuntimeException::new);
        for (Segment segment : currentSegments.getSegments()) {
            if (!writers.containsKey(segment)) {
                SegmentOutputStream out = outputStreamFactory.createOutputStreamForSegment(segment);
                writers.put(segment, out);
            }
        }
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
