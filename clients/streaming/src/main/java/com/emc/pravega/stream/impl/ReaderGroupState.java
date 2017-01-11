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
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

class ReaderGroupState implements Revisioned {

    private final String scopedSynchronizerStream;
    @GuardedBy("$lock")
    private final List<String> streams;
    @GuardedBy("$lock")
    private Revision revision;
    @GuardedBy("$lock")
    private final Map<String, Long> distanceToTail = new HashMap<>();
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
            Set<Segment> segments = state.assignedSegments.get(consumerId);
            if (segments != null) {
                state.assignedSegments.put(consumerId, new HashSet<>());
            }
            state.distanceToTail.putIfAbsent(consumerId, Long.MAX_VALUE);
        }
    }
    
    @RequiredArgsConstructor
    static class RemoveReader extends ReaderGroupStateUpdate {
        private final String consumerId;

        @Override
        void update(ReaderGroupState state) {
            Set<Segment> segments = state.assignedSegments.get(consumerId);
            if (segments != null) {
                if (!segments.isEmpty()) {
                    throw new IllegalStateException(
                            "Attempted to remove a reader without first releasing its segments. " + consumerId);
                }
                state.assignedSegments.remove(consumerId);
            }
            state.distanceToTail.remove(consumerId);
        }
    }

    @RequiredArgsConstructor
    static class ReleaseSegment extends ReaderGroupStateUpdate {
        private final String consumerId;
        private final Segment segment;
        private final long offset;

        @Override
        void update(ReaderGroupState state) {
            Set<Segment> segments = state.assignedSegments.get(consumerId);
            Preconditions.checkState(segments != null);
            if (segments.remove(segment)) {
                throw new IllegalStateException(
                        "Segment: " + segment + " is not owned by " + consumerId + ". " + state);
            }
            state.unassignedSegments.put(segment, offset);
        }
    }

    @RequiredArgsConstructor
    static class AquireSegment extends ReaderGroupStateUpdate {
        private final String consumerId;
        private final Segment segment;

        @Override
        void update(ReaderGroupState state) {
            Set<Segment> segments = state.assignedSegments.get(consumerId);
            Preconditions.checkState(segments != null);
            if (state.unassignedSegments.remove(segment) != null) {
                throw new IllegalStateException("Segment: " + segment + " is not unassigned. " + state);
            } 
            segments.add(segment);
        }
    }
    
    @RequiredArgsConstructor
    static class UpdateDistanceToTail extends ReaderGroupStateUpdate {
        private final String consumerId;
        private final long distanceToTail;
        @Override
        void update(ReaderGroupState state) {
            state.distanceToTail.put(consumerId, distanceToTail);
        }
    }
    
}
