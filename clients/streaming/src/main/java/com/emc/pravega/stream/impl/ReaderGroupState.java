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

import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revision;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.Segment;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.val;

class ReaderGroupState implements Revisioned {

    private final String scopedSynchronizerStream;
    @GuardedBy("$lock")
    private final List<String> streams;
    @GuardedBy("$lock")
    private Revision revision;
    @GuardedBy("$lock")
    private final Map<String, Long> distanceToTail = new HashMap<>();
    @GuardedBy("$lock")
    private final Map<Segment, Set<Integer>> futureSegments = new HashMap<>();
    @GuardedBy("$lock")
    private final Map<String, Set<Segment>> assignedSegments = new HashMap<>();
    @GuardedBy("$lock")
    private final Map<Segment, Long> unassignedSegments = new HashMap<>();

    ReaderGroupState (String scopedSynchronizerStream, Revision revision, List<String> streams) {
        this.scopedSynchronizerStream = scopedSynchronizerStream;
        this.revision = revision;
        this.streams = streams;
    }
    
    /**
     * @return A map from Reader to a relative measure of how much data they have to process. The
     *         scale is calibrated to where 1.0 is equal to the largest segment.
     */
    @Synchronized
    Map<String, Double> getRelitiveSizes() {
        Long maxTime = distanceToTail.values().stream().max(Long::compareTo).orElse(null);
        Map<String, Double> result = new HashMap<>();
        distanceToTail.forEach((host, size) -> {
            result.put(host, size / (double) maxTime);
        });
        return result;
    }
    
    @Synchronized
    int getNumberOfReaders() {
        return assignedSegments.size();
    }
    
    /**
     * @return The 0 indexed ranking of the requested reader in the reader group in terms of amount of keyspace assigned to it.
     */
    @Synchronized
    int getRanking(String reader) {
        List<String> sorted = distanceToTail.entrySet()
                                   .stream()
                                   .sorted((o1, o2) -> Long.compare(o1.getValue(), o2.getValue()))
                                   .map(e -> e.getKey())
                                   .collect(Collectors.toList());
        return sorted.size() - sorted.indexOf(reader);
    }

    @Override
    @Synchronized
    public Revision getRevision() {
        return revision;
    }
    
    @Override
    public String getScopedStreamName() {
        return scopedSynchronizerStream;
    }
    
    /**
     * Returns the list of segments assigned to the requested reader, or null if this reader does not exist.
     */
    @Synchronized
    Set<Segment> getSegments(String reader) {
        Set<Segment> segments = assignedSegments.get(reader);
        if (segments == null) {
            return null;
        }
        return Collections.unmodifiableSet(segments);
    }
    
    @Synchronized
    public Map<Segment,Long> getUnassignedSegments() {
        return Collections.unmodifiableMap(unassignedSegments);
    }
    
    @RequiredArgsConstructor
    static class ReaderGroupStateInit implements InitialUpdate<ReaderGroupState> {
        private final List<String> streams;
        
        @Override
        public ReaderGroupState create(String scopedStreamName, Revision revision) {
            return new ReaderGroupState(scopedStreamName, revision, streams);
        }
    }
    
    static abstract class ReaderGroupStateUpdate implements Update<ReaderGroupState> {
        @Override
        public ReaderGroupState applyTo(ReaderGroupState oldState, Revision newRevision) {
            synchronized (oldState.$lock) {
                update(oldState);
                oldState.revision = newRevision;
            }
            return oldState;
        }

        abstract void update(ReaderGroupState state);
    }
    
    @RequiredArgsConstructor
    static class AddReader extends ReaderGroupStateUpdate {
        private final String consumerId;

        @Override
        void update(ReaderGroupState state) {
            Set<Segment> oldPos = state.assignedSegments.putIfAbsent(consumerId, new HashSet<>());
            if (oldPos != null) {
                throw new IllegalStateException("Attempted to add a reader that is already online. " + consumerId);
            }
            state.distanceToTail.putIfAbsent(consumerId, Long.MAX_VALUE);
        }
    }
    
    @RequiredArgsConstructor
    static class RemoveReader extends ReaderGroupStateUpdate {
        private final String consumerId;
        private final PositionImpl lastPosition;

        @Override
        void update(ReaderGroupState state) {
            Set<Segment> assignedSegments = state.assignedSegments.remove(consumerId);
            if (assignedSegments != null) {
                val iter = assignedSegments.iterator();
                while (iter.hasNext()) {
                    Segment segment = iter.next();
                    Long offset = lastPosition.getOffsetForOwnedSegment(segment);
                    Preconditions.checkState(offset != null,
                                             "No offset in lastPosition for assigned segment: " + segment);
                    state.unassignedSegments.put(segment, offset);
                    iter.remove();
                }
            }
            state.distanceToTail.remove(consumerId);
        }
    }

    /**
     * Release a currently owned segment.
     */
    @RequiredArgsConstructor
    static class ReleaseSegment extends ReaderGroupStateUpdate {
        private final String consumerId;
        private final Segment segment;
        private final long offset;

        @Override
        void update(ReaderGroupState state) {
            Set<Segment> assigned = state.assignedSegments.get(consumerId);
            if (!assigned.remove(segment)) {
                throw new IllegalStateException(
                        consumerId + " asked to release a segment that was not assigned to it " + segment);
            }
            state.unassignedSegments.put(segment, offset);
        }
    }

    /**
     * Acquire a currently unassigned segment.
     */
    @RequiredArgsConstructor
    static class AquireSegment extends ReaderGroupStateUpdate {
        private final String consumerId;
        private final Segment segment;

        @Override
        void update(ReaderGroupState state) {
            Set<Segment> assigned = state.assignedSegments.get(consumerId);
            Preconditions.checkState(assigned != null);
            if (state.unassignedSegments.remove(segment) != null) {
                throw new IllegalStateException("Segment: " + segment + " is not unassigned. " + state);
            }
            assigned.add(segment);
        }
    }
    
    /**
     * Update the size of this consumers backlog for load balancing purposes. 
     */
    @RequiredArgsConstructor
    static class UpdateDistanceToTail extends ReaderGroupStateUpdate {
        private final String consumerId;
        private final long distanceToTail;
        @Override
        void update(ReaderGroupState state) {
            state.distanceToTail.put(consumerId, distanceToTail);
        }
    }
    
    /**
     * Updates a position object when the reader has completed a segment.
     */
    @RequiredArgsConstructor
    static class SegmentCompleted extends ReaderGroupStateUpdate {
        private final String consumerId;
        private final Segment segmentCompleted;
        private final Map<Segment, List<Integer>> successorsMappedToTheirPredecessors;
        
        @Override
        void update(ReaderGroupState state) {
            Set<Segment> assigned = state.assignedSegments.get(consumerId);
            Preconditions.checkState(assigned != null);
            if (!assigned.remove(segmentCompleted)) {
                throw new IllegalStateException(
                        consumerId + " asked to complete a segment that was not assigned to it " + segmentCompleted);
            }
            for (Entry<Segment, List<Integer>> entry : successorsMappedToTheirPredecessors.entrySet()) {
                Set<Integer> requiredToComplete = state.futureSegments.getOrDefault(entry.getKey(), new HashSet<>());
                requiredToComplete.addAll(entry.getValue());
            }
            for (Set<Integer> requiredToComplete : state.futureSegments.values()) {
                requiredToComplete.remove(segmentCompleted);
            }
            val iter = state.futureSegments.entrySet().iterator();
            while(iter.hasNext()) {
                Entry<Segment, Set<Integer>> entry = iter.next();
                if (entry.getValue().isEmpty()) {
                    state.unassignedSegments.put(entry.getKey(), 0L);
                    iter.remove();
                }
            }
        }
    }
    
    
    
}
