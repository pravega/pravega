/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Sequence;
import com.emc.pravega.stream.impl.ReaderGroupState.AddReader;
import com.emc.pravega.stream.impl.ReaderGroupState.AquireSegment;
import com.emc.pravega.stream.impl.ReaderGroupState.ReaderGroupStateUpdate;
import com.emc.pravega.stream.impl.ReaderGroupState.ReleaseSegment;
import com.emc.pravega.stream.impl.ReaderGroupState.RemoveReader;
import com.emc.pravega.stream.impl.ReaderGroupState.SegmentCompleted;
import com.emc.pravega.stream.impl.ReaderGroupState.UpdateDistanceToTail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
public class ReaderGroupStateManager {
    
    private static final long TIME_UNIT_MILLIS = 1000;
    private static final long UPDATE_TIME_MILLIS = 30000;
    private static final long ASSUMED_LAG_MILLIS = 30000;
    private final String readerId;
    private final StateSynchronizer<ReaderGroupState> sync;
    private final Controller controller;
    private final AtomicLong nextReleaseTime = new AtomicLong();
    private final AtomicLong nextAquireTime = new AtomicLong();

    void initializeReadererGroup(Map<Segment, Long> segments) {
        sync.initialize(new ReaderGroupState.ReaderGroupStateInit(segments)); 
    }
    
    void initializeReader() {
        sync.updateState(state -> {
            if (state.getSegments(readerId) == null) {
                return Collections.singletonList(new AddReader(readerId));
            } else {
                return null;
            }
        });
    }
    
    /**
     * Shuts down a reader, releasing all of its segments. The reader should cease all operations.
     * @param lastPosition The last position the reader successfully read from.
     */
    void readerShutdown(PositionImpl lastPosition) {
        sync.updateState(state -> {
            Set<Segment> segments = state.getSegments(readerId);
            if (segments == null) {
                return null;
            }
            if (!lastPosition.getOwnedSegments().containsAll(segments)) {
                throw new IllegalArgumentException(
                        "When shutting down a reader: Given position does not match the segments it was assigned: \n"
                                + segments + " \n vs \n " + lastPosition.getOwnedSegments());
            }
            return Collections.singletonList(new RemoveReader(readerId, lastPosition));
        });
    }
    
    /**
     * Handles a segment being completed by calling the controller to gather all successors to the completed segment.
     */
    void handleEndOfSegment(Segment segmentCompleted) {
        val successors = getAndHandleExceptions(controller.getSegmentsImmediatlyFollowing(segmentCompleted),
                                                RuntimeException::new);
        sync.updateState(state -> {
            return Collections.singletonList(new SegmentCompleted(readerId, segmentCompleted, successors));
        });
        nextAquireTime.set(0);
    }

    /**
     * If a segment should be released because there is inequity in the distribution of segments and
     * this reader has not done so in a while, this returns the segment that should be released.
     */
    Segment findSegmentToReleaseIfRequired() {
        if (shouldReleaseSegment()) {
            return findSegmentToRelease();
        } else { 
            return null;
        }
    }
    
    private boolean shouldReleaseSegment() {
        long releaseTime;
        do { // Loop handles race with another thread releasing a segment.
            releaseTime = nextReleaseTime.get();
            if (System.currentTimeMillis() < releaseTime) {
                return false;
            }
            if (!doesReaderOwnTooManySegments(sync.getState())) {
                return false;
            }
        } while (!nextReleaseTime.compareAndSet(releaseTime, System.currentTimeMillis() + UPDATE_TIME_MILLIS));
        return true;
    }

    /**
     * Returns true if this reader owns multiple segments and has more than a full segment more than
     * the reader with the least assigned to it.
     */
    private boolean doesReaderOwnTooManySegments(ReaderGroupState state) {
        Map<String, Double> sizesOfAssignemnts = state.getRelativeSizes();
        Set<Segment> assignedSegments = state.getSegments(readerId);
        if (sizesOfAssignemnts.isEmpty() || assignedSegments == null || assignedSegments.size() <= 1) {
            return false;
        }
        double min = sizesOfAssignemnts.values().stream().min(Double::compareTo).get();
        return sizesOfAssignemnts.get(readerId) > min + 1.0;
    }

    /**
     * Given a set of segments returns one to release. The one returned is arbitrary.
     */
    private Segment findSegmentToRelease() {
        Set<Segment> segments = sync.getState().getSegments(readerId);
        return segments.stream().max((s1, s2) -> Integer.compare(s1.getSegmentNumber(), s2.getSegmentNumber())).orElse(null);
    }

    /**
     * Releases a segment to another reader. This reader should no longer read from the segment. 
     * @param segment The segment to be released
     * @param lastOffset The offset from which the new owner should start reading from.
     * @param pos The current position of the reader
     * @return a boolean indicating if the segment was successfully released.
     */
    boolean releaseSegment(Segment segment, long lastOffset, Sequence pos) {
        sync.updateState(state -> {
            Set<Segment> segments = state.getSegments(readerId);
            if (segments == null || !segments.contains(segment) || !doesReaderOwnTooManySegments(state)) {
                return null;
            }
            List<ReaderGroupStateUpdate> result = new ArrayList<>(2);
            result.add(new ReleaseSegment(readerId, segment, lastOffset));
            long distanceToTail = computeDistanceToTail(pos, segments.size() - 1);
            result.add(new UpdateDistanceToTail(readerId, distanceToTail));
            return result;
        });
        ReaderGroupState state = sync.getState();
        nextReleaseTime.set(calculateReleaseTime(state));
        return !state.getSegments(readerId).contains(segment);
    }

    private long calculateReleaseTime(ReaderGroupState state) {
        return System.currentTimeMillis() + (1 + state.getRanking(readerId)) * TIME_UNIT_MILLIS;
    }

    /**
     * If there are unassigned segments and this host has not acquired one in a while, acquires them.
     * @return A map from the new segment that was acquired to the offset to begin reading from within the segment.
     */
    Map<Segment, Long> aquireNewSegmentsIfNeeded(Sequence lastRead) {
        if (shouldAquireSegment()) {
            return aquireSegment(lastRead);
        } else {
            return null;
        }
    }
    
    private boolean shouldAquireSegment() {
        long aquireTime;
        do { // Loop handles race with another thread acquiring a segment.
            aquireTime = nextAquireTime.get();
            if (System.currentTimeMillis() < aquireTime) {
                return false;
            }
            if (sync.getState().getUnassignedSegments().isEmpty()) {
                return false;
            }
        } while (!nextAquireTime.compareAndSet(aquireTime, System.currentTimeMillis() + UPDATE_TIME_MILLIS));
        return true;
    }

    private Map<Segment, Long> aquireSegment(Sequence lastRead) {
        AtomicReference<Map<Segment, Long>> result = new AtomicReference<>();
        sync.updateState(state -> {
            Map<Segment, Long> unassignedSegments = state.getUnassignedSegments();
            if (unassignedSegments.isEmpty()) {
                return null;
            }
            int toAquire = Math.max(1, unassignedSegments.size() / state.getNumberOfReaders());
            Map<Segment, Long> aquired = new HashMap<>(toAquire);
            List<ReaderGroupStateUpdate> updates = new ArrayList<>(toAquire);
            Iterator<Entry<Segment, Long>> iter = unassignedSegments.entrySet().iterator();
            for (int i = 0; i < toAquire; i++) {
                Entry<Segment, Long> segment = iter.next();
                aquired.put(segment.getKey(), segment.getValue());
                updates.add(new AquireSegment(readerId, segment.getKey()));
            }
            long toTail = computeDistanceToTail(lastRead, state.getSegments(readerId).size() + aquired.size());
            updates.add(new UpdateDistanceToTail(readerId, toTail));
            result.set(aquired);
            return updates;
        });
        nextAquireTime.set(calculateAquireTime(sync.getState()));
        return result.get();
    }

    private long calculateAquireTime(ReaderGroupState state) {
        return System.currentTimeMillis()
                + (state.getNumberOfReaders() - state.getRanking(readerId)) * TIME_UNIT_MILLIS;
    }

    private long computeDistanceToTail(Sequence lastRead, int numSegments) {
        return Math.max(ASSUMED_LAG_MILLIS, System.currentTimeMillis() - lastRead.getHighOrder()) * numSegments;
    }
}
