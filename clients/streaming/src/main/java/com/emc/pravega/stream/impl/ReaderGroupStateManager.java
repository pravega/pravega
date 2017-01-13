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
import com.emc.pravega.stream.impl.ReaderGroupState.UpdateDistanceToTail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
public class ReaderGroupStateManager {
    
    private static final long TIME_UNIT_MILLIS = 1000;
    private static final long UPDATE_TIME_MILLIS = 30000;
    private static final long ASSUMED_LAG_MILLIS = 30000;
    private final String consumerId;
    private final StateSynchronizer<ReaderGroupState> sync;
    private final Controller controller;
    private final AtomicLong nextReleaseTime = new AtomicLong();
    private final AtomicLong nextAquireTime = new AtomicLong();

    void initializeReader() {
        sync.updateState(state -> {
            if (state.getSegments(consumerId) == null) {
                return Collections.singletonList(new AddReader(consumerId, PositionImpl.createEmptyPosition()));
            } else {
                return null;
            }
        });
    }
    
    void readerShutdown(PositionImpl lastPosition) {
        sync.updateState(state -> {
            Set<Segment> segments = state.getSegments(consumerId);
            if (segments == null) {
                return null;
            }
            if (!lastPosition.getOwnedSegments().containsAll(segments)) {
                throw new IllegalArgumentException("When shutting down a reader: provided a position "
                        + " that does not match the segments did not match the ones they were assigned: \n" + segments
                        + " \n vs \n " + lastPosition.getOwnedSegments());
            }
            return Collections.singletonList(new RemoveReader(consumerId, lastPosition));
        });
    }
    
    void handleEndOfSegment(PositionImpl position) {
        //TODO: finish implementing.
    }

    Segment shouldReleaseSegment() {
        long releaseTime = nextReleaseTime.get();
        if (System.currentTimeMillis() < releaseTime) {
            return null;
        }
        ReaderGroupState state = sync.getState();
        if (!shouldReleaseSegment(state)) {
            return null;
        }
        if (nextReleaseTime.compareAndSet(releaseTime, System.currentTimeMillis() + UPDATE_TIME_MILLIS)) {
            return null; // Race. Another thread already is releasing segment.
        }
        return findSegmentToRelease(state.getPosition(consumerId));
    }

    private boolean shouldReleaseSegment(ReaderGroupState state) {
        Map<String, Double> sizes = state.getRelitiveSizes();
        if (sizes.isEmpty()) {
            return false;
        }
        double min = sizes.values().stream().min(Double::compareTo).get();
        if (sizes.get(consumerId) < min + 1.0) {
            return false;
        }
        Set<Segment> segments = state.getSegments(consumerId);
        if (segments == null) {
            return false;
        }
        return segments.size() > 1;
    }

    private Segment findSegmentToRelease(PositionImpl position) {
        List<Segment> sorted = position.getOwnedSegments().stream().sorted().collect(Collectors.toList());
        Map<Integer, List<FutureSegment>> reverseMap = position.getFutureOwnedSegments()
                                                               .stream()
                                                               .collect(Collectors.groupingBy((
                                                                       FutureSegment s) -> s.getPrecedingNumber()));
       return sorted.stream().max(new Comparator<Segment>() {
            @Override
            public int compare(Segment s1, Segment s2) {
                List<FutureSegment> s1futures = reverseMap.get(s1.getSegmentNumber());
                List<FutureSegment> s2futures = reverseMap.get(s2.getSegmentNumber());
                int s1Count = s1futures == null ? 0 : s1futures.size();
                int s2Count = s2futures == null ? 0 : s2futures.size();
                if (s1Count == s2Count) {
                    return Integer.compare(s1.getSegmentNumber(), s2.getSegmentNumber());
                } else {
                    return -Integer.compare(s1Count, s2Count);
                }
            }
       }).orElse(null);
      
    }

    boolean releaseSegment(PositionImpl currentPos, Segment segment, long lastOffset, Sequence pos) {
        sync.updateState(state -> {
            Set<Segment> segments = state.getSegments(consumerId);
            if (!shouldReleaseSegment(state) || segments == null || !segments.contains(segment)) {
                return null;
            }
            List<ReaderGroupStateUpdate> result = new ArrayList<>(2);
            PositionImpl newPos = currentPos.copyWithout(segment);
            result.add(new ReleaseSegment(consumerId, newPos, segment, lastOffset));
            long distanceToTail = computeDistanceToTail(pos, segments.size() - 1);
            result.add(new UpdateDistanceToTail(consumerId, distanceToTail));
            return result;
        });
        ReaderGroupState state = sync.getState();
        nextReleaseTime.set(calculateReleaseTime(state));
        return !state.getSegments(consumerId).contains(segment);
    }

    private long calculateReleaseTime(ReaderGroupState state) {
        return System.currentTimeMillis() + (1 + state.getRanking(consumerId)) * TIME_UNIT_MILLIS;
    }

    Map<Segment, Long> aquireNewSegmentsIfNeeded(Sequence lastRead) {
        long aquireTime = nextAquireTime.get();
        if (System.currentTimeMillis() < aquireTime) {
            return null;
        }
        ReaderGroupState state = sync.getState();
        if (state.getUnassignedSegments().isEmpty()) {
            return null;
        }
        if (nextAquireTime.compareAndSet(aquireTime, System.currentTimeMillis() + UPDATE_TIME_MILLIS)) {
            return null; // Race. Another thread already is acquiring a segment.
        }
        return aquireSegment(lastRead);
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
            PositionImpl pos = state.getPosition(consumerId);
            val iter = unassignedSegments.entrySet().iterator();
            for (int i = 0; i < toAquire; i++) {
                Entry<Segment, Long> segment = iter.next();
                aquired.put(segment.getKey(), segment.getValue());
                pos = pos.copyWith(segment.getKey(), segment.getValue());
                updates.add(new AquireSegment(consumerId, pos, segment.getKey()));
            }
            long toTail = computeDistanceToTail(lastRead, state.getSegments(consumerId).size() + aquired.size());
            updates.add(new UpdateDistanceToTail(consumerId, toTail));
            result.set(aquired);
            return updates;
        });
        nextAquireTime.set(calculateAquireTime(sync.getState()));
        return result.get();
    }

    private long calculateAquireTime(ReaderGroupState state) {
        return System.currentTimeMillis()
                + (state.getNumberOfReaders() - state.getRanking(consumerId)) * TIME_UNIT_MILLIS;
    }

    private long computeDistanceToTail(Sequence lastRead, int numSegments) {
        return Math.max(ASSUMED_LAG_MILLIS, System.currentTimeMillis() - lastRead.getHighOrder()) * numSegments;
    }
}
