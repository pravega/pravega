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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revision;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.Update;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.common.Exceptions;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.val;

/**
 * This class encapsulates the state machine of a reader group. The class represents the full state,
 * and each of the nested classes are state transitions that can occur.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ReaderGroupState implements Revisioned {

    private static final long ASSUMED_LAG_MILLIS = 30000;
    private final String scopedSynchronizerStream;
    @Getter
    private final ReaderGroupConfig config;
    @GuardedBy("$lock")
    private Revision revision;
    @GuardedBy("$lock")
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final CheckpointState checkpointState;
    @GuardedBy("$lock")
    private final Map<String, Long> distanceToTail;
    @GuardedBy("$lock")
    private final Map<Segment, Set<Integer>> futureSegments;
    @GuardedBy("$lock")
    private final Map<String, Map<Segment, Long>> assignedSegments;
    @GuardedBy("$lock")
    private final Map<Segment, Long> unassignedSegments;
    private final Map<Segment, Long> endSegments;
    
    ReaderGroupState(String scopedSynchronizerStream, Revision revision, ReaderGroupConfig config, Map<Segment, Long> segmentsToOffsets,
                     Map<Segment, Long> endSegments) {
        Exceptions.checkNotNullOrEmpty(scopedSynchronizerStream, "scopedSynchronizerStream");
        Preconditions.checkNotNull(revision);
        Preconditions.checkNotNull(config);
        Exceptions.checkNotNullOrEmpty(segmentsToOffsets.entrySet(), "segmentsToOffsets");
        this.scopedSynchronizerStream = scopedSynchronizerStream;
        this.config = config;
        this.revision = revision;
        this.checkpointState = new CheckpointState();
        this.distanceToTail = new HashMap<>();
        this.futureSegments = new HashMap<>();
        this.assignedSegments = new HashMap<>();
        this.unassignedSegments = new LinkedHashMap<>(segmentsToOffsets);
        this.endSegments = Collections.unmodifiableMap(endSegments);
    }
    
    /**
     * @return A map from Reader to a relative measure of how much data they have to process. The
     *         scale is calibrated to where 1.0 is equal to the largest segment.
     */
    @Synchronized
    Map<String, Double> getRelativeSizes() {
        long maxDistance = Long.MIN_VALUE;
        Map<String, Double> result = new HashMap<>();
        for (Entry<String, Long> entry : distanceToTail.entrySet()) {
            Set<Segment> segments = assignedSegments.get(entry.getKey()).keySet();
            if (segments != null && !segments.isEmpty()) {
                maxDistance = Math.max(Math.max(ASSUMED_LAG_MILLIS, entry.getValue()), maxDistance);
            }
        }
        for (Entry<String, Map<Segment, Long>> entry : assignedSegments.entrySet()) {
            if (entry.getValue().isEmpty()) {
                result.put(entry.getKey(), 0.0);
            } else {
                Long distance = Math.max(ASSUMED_LAG_MILLIS, distanceToTail.get(entry.getKey()));
                result.put(entry.getKey(), entry.getValue().size() * distance / (double) maxDistance);
            }
        }
        return result;
    }
    
    @Synchronized
    int getNumberOfReaders() {
        return assignedSegments.size();
    }
    
    @Synchronized
    public Set<String> getOnlineReaders() {
        return new HashSet<>(assignedSegments.keySet());
    }
    
    /**
     * @return The 0 indexed ranking of the requested reader in the reader group in terms of amount
     *         of keyspace assigned to it, or -1 if the reader is not part of the group.
     *         The reader with the most keyspace will be 0 and the reader with the least keyspace will be numReaders-1.
     */
    @Synchronized
    int getRanking(String reader) {
        List<String> sorted = getRelativeSizes().entrySet()
                                   .stream()
                                   .sorted((o1, o2) -> -Double.compare(o1.getValue(), o2.getValue()))
                                   .map(Entry::getKey)
                                   .collect(Collectors.toList());
        return sorted.indexOf(reader);
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
        Map<Segment, Long> segments = assignedSegments.get(reader);
        if (segments == null) {
            return null;
        }
        return new HashSet<>(segments.keySet());
    }
    
    @Synchronized
    Map<Stream, Map<Segment, Long>> getPositions() {
        Map<Stream, Map<Segment, Long>> result = new HashMap<>();
        for (Entry<Segment, Long> entry : unassignedSegments.entrySet()) {
            result.computeIfAbsent(entry.getKey().getStream(), s -> new HashMap<>()).put(entry.getKey(), entry.getValue());
        }
        for (Map<Segment, Long> assigned : assignedSegments.values()) {
            for (Entry<Segment, Long> entry : assigned.entrySet()) {
                result.computeIfAbsent(entry.getKey().getStream(), s -> new HashMap<>()).put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
    
    @Synchronized
    int getNumberOfUnassignedSegments() {
        return unassignedSegments.size();
    }
    
    @Synchronized
    Map<Segment, Long> getUnassignedSegments() {
        return new HashMap<>(unassignedSegments);
    }

    Map<Segment, Long> getEndSegments() {
        return Collections.unmodifiableMap(new HashMap<>(endSegments));
    }

    @Synchronized
    boolean isReaderOnline(String reader) {
        return assignedSegments.get(reader) != null;
    }

    /**
     * Returns the number of segments currently being read from and that are unassigned within the reader group.
     */
    @Synchronized
    public int getNumberOfSegments() {
        return assignedSegments.values().stream().mapToInt(Map::size).sum() + unassignedSegments.size();
    }
    
    @Synchronized
    Set<String> getStreamNames() {
        Set<String> result = new HashSet<>();
        for (Map<Segment, Long> segments : assignedSegments.values()) {
            for (Segment segment : segments.keySet()) {
                result.add(segment.getStreamName());
            }
        }
        for (Segment segment : unassignedSegments.keySet()) {
            result.add(segment.getStreamName());
        }
        return result;
    }
    
    @Synchronized
    String getCheckpointForReader(String readerName) {
        return checkpointState.getCheckpointForReader(readerName);
    }
    
    @Synchronized
    boolean isCheckpointComplete(String checkpointId) {
        return checkpointState.isCheckpointComplete(checkpointId);
    }
    
    @Synchronized
    Map<Segment, Long> getPositionsForCompletedCheckpoint(String checkpointId) {
        return checkpointState.getPositionsForCompletedCheckpoint(checkpointId);
    }

    @Synchronized
    Optional<Map<Stream, Map<Segment, Long>>> getPositionsForLastCompletedCheckpoint() {
        Optional<Map<Segment, Long>> positions = checkpointState.getPositionsForLatestCompletedCheckpoint();
        if (positions.isPresent()) {
            Map<Stream, Map<Segment, Long>> result = new HashMap<>();
            for (Entry<Segment, Long> entry : positions.get().entrySet()) {
                result.computeIfAbsent(entry.getKey().getStream(), s -> new HashMap<>()).put(entry.getKey(), entry.getValue());
            }
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }
    
    @Synchronized
    boolean hasOngoingCheckpoint() {
        return checkpointState.hasOngoingCheckpoint();
    }

    /**
     * This functions returns true if the readers part of reader group for sealed streams have completely read the data.
     *
     * @return true if end of data. false if there are segments to be read from.
     */
    @Synchronized
    public boolean isEndOfData() {
        return futureSegments.isEmpty() && unassignedSegments.isEmpty() &&
                assignedSegments.values().stream().allMatch(Map::isEmpty);
    }

    @Override
    @Synchronized
    public String toString() {
        StringBuffer sb = new StringBuffer("ReaderGroupState{ ");
        sb.append(checkpointState.toString());
        sb.append(" futureSegments: ");
        sb.append(futureSegments);
        sb.append(" assignedSegments: ");
        sb.append(assignedSegments);
        sb.append(" unassignedSegments: ");
        sb.append(unassignedSegments);
        sb.append(" }");
        return sb.toString();
    }
    
    @RequiredArgsConstructor
    static class ReaderGroupStateInit implements InitialUpdate<ReaderGroupState>, Serializable {
        private static final long serialVersionUID = 1L;

        private final ReaderGroupConfig config;
        private final Map<Segment, Long> segments;
        private final Map<Segment, Long> endSegments;
        
        @Override
        public ReaderGroupState create(String scopedStreamName, Revision revision) {
            return new ReaderGroupState(scopedStreamName, revision, config, segments, endSegments);
        }
    }
    
    static class CompactReaderGroupState implements InitialUpdate<ReaderGroupState>, Serializable {
        private static final long serialVersionUID = 1L;

        private final ReaderGroupConfig config;
        private final CheckpointState checkpointState;
        private final Map<String, Long> distanceToTail;
        private final Map<Segment, Set<Integer>> futureSegments;
        private final Map<String, Map<Segment, Long>> assignedSegments;
        private final Map<Segment, Long> unassignedSegments;
        private final Map<Segment, Long> endSegments;
        
        CompactReaderGroupState(ReaderGroupState state) {
            synchronized (state.$lock) {
                config = state.config;
                checkpointState = state.checkpointState.copy();
                distanceToTail = new HashMap<>(state.distanceToTail);
                futureSegments = new HashMap<>();
                for (Entry<Segment, Set<Integer>> entry : state.futureSegments.entrySet()) {
                    futureSegments.put(entry.getKey(), new HashSet<>(entry.getValue()));
                }
                assignedSegments = new HashMap<>();
                for (Entry<String, Map<Segment, Long>> entry : state.assignedSegments.entrySet()) {
                    assignedSegments.put(entry.getKey(), new HashMap<>(entry.getValue()));
                }
                unassignedSegments = new LinkedHashMap<>(state.unassignedSegments);
                endSegments = state.endSegments;
            }
        }
        
        @Override
        public ReaderGroupState create(String scopedStreamName, Revision revision) {
            return new ReaderGroupState(scopedStreamName, config, revision, checkpointState, distanceToTail,
                                        futureSegments, assignedSegments, unassignedSegments, endSegments);
        }
    }
    
    /**
     * Abstract class from which all state updates extend.
     */
    static abstract class ReaderGroupStateUpdate implements Update<ReaderGroupState>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public ReaderGroupState applyTo(ReaderGroupState oldState, Revision newRevision) {
            synchronized (oldState.$lock) {
                update(oldState);
                oldState.revision = newRevision;
            }
            return oldState;
        }

        /**
         * Changes the state to reflect the update.
         * Note that a lock while this method is called so only one update will be applied at a time.
         * Implementations of this should not call any methods outside of this class.
         * @param state The state to be updated.
         */
        abstract void update(ReaderGroupState state);
    }
    
    /**
     * Adds a reader to the reader group. (No segments are initially assigned to it)
     */
    @RequiredArgsConstructor
    static class AddReader extends ReaderGroupStateUpdate {
        private static final long serialVersionUID = 1L;
        private final String readerId;

        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            Map<Segment, Long> oldPos = state.assignedSegments.putIfAbsent(readerId, new HashMap<>());
            if (oldPos != null) {
                throw new IllegalStateException("Attempted to add a reader that is already online: " + readerId);
            }
            state.distanceToTail.putIfAbsent(readerId, Long.MAX_VALUE);
        }
    }
    
    /**
     * Remove a reader from reader group, releasing all segments it owned.
     */
    @RequiredArgsConstructor
    static class RemoveReader extends ReaderGroupStateUpdate {
        private static final long serialVersionUID = 1L;
        private final String readerId;
        private final PositionInternal lastPosition;
        
        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            Map<Segment, Long> assignedSegments = state.assignedSegments.remove(readerId);
            Map<Segment, Long> finalPositions = new HashMap<>();
            if (assignedSegments != null) {
                val iter = assignedSegments.entrySet().iterator();
                while (iter.hasNext()) {
                    Entry<Segment, Long> entry = iter.next();
                    Segment segment = entry.getKey();
                    Long offset;
                    if (lastPosition == null) {
                        offset = entry.getValue();
                    } else {
                        offset = lastPosition.getOffsetForOwnedSegment(segment);
                        Preconditions.checkState(offset != null,
                                "No offset in lastPosition for assigned segment: " + segment);
                    }
                    finalPositions.put(segment, offset);
                    state.unassignedSegments.put(segment, offset);
                    iter.remove();
                }
            }
            state.distanceToTail.remove(readerId);
            state.checkpointState.removeReader(readerId, finalPositions);
        }
    }

    /**
     * Release a currently owned segment.
     */
    @RequiredArgsConstructor
    static class ReleaseSegment extends ReaderGroupStateUpdate {
        private static final long serialVersionUID = 1L;
        private final String readerId;
        private final Segment segment;
        private final long offset;

        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            Map<Segment, Long> assigned = state.assignedSegments.get(readerId);
            Preconditions.checkState(assigned != null, "%s is not part of the readerGroup", readerId);
            if (assigned.remove(segment) == null) {
                throw new IllegalStateException(
                        readerId + " asked to release a segment that was not assigned to it " + segment);
            }
            state.unassignedSegments.put(segment, offset);
        }
    }

    /**
     * Acquire a currently unassigned segment.
     */
    @RequiredArgsConstructor
    static class AcquireSegment extends ReaderGroupStateUpdate {
        private static final long serialVersionUID = 1L;
        private final String readerId;
        private final Segment segment;

        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            Map<Segment, Long> assigned = state.assignedSegments.get(readerId);
            Preconditions.checkState(assigned != null, "%s is not part of the readerGroup", readerId);
            Long offset = state.unassignedSegments.remove(segment);
            if (offset == null) {
                throw new IllegalStateException("Segment: " + segment + " is not unassigned. " + state);
            }
            assigned.put(segment, offset);
        }
    }
    
    /**
     * Update the size of this reader's backlog for load balancing purposes. 
     */
    @RequiredArgsConstructor
    static class UpdateDistanceToTail extends ReaderGroupStateUpdate {
        private static final long serialVersionUID = 1L;
        private final String readerId;
        private final long distanceToTail;
        
        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            state.distanceToTail.put(readerId, Math.max(ASSUMED_LAG_MILLIS, distanceToTail));
        }
    }
    
    /**
     * Updates a position object when the reader has completed a segment.
     */
    @RequiredArgsConstructor
    static class SegmentCompleted extends ReaderGroupStateUpdate {
        private static final long serialVersionUID = 1L;
        private final String readerId;
        private final Segment segmentCompleted;
        private final Map<Segment, List<Integer>> successorsMappedToTheirPredecessors; //Immutable
        
        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            Map<Segment, Long> assigned = state.assignedSegments.get(readerId);
            Preconditions.checkState(assigned != null, "%s is not part of the readerGroup", readerId);
            if (assigned.remove(segmentCompleted) == null) {
                throw new IllegalStateException(
                        readerId + " asked to complete a segment that was not assigned to it " + segmentCompleted);
            }
            for (Entry<Segment, List<Integer>> entry : successorsMappedToTheirPredecessors.entrySet()) {
                if (!state.futureSegments.containsKey(entry.getKey())) {
                    Set<Integer> requiredToComplete = new HashSet<>(entry.getValue());
                    state.futureSegments.put(entry.getKey(), requiredToComplete);
                }
            }
            for (Set<Integer> requiredToComplete : state.futureSegments.values()) {
                requiredToComplete.remove(segmentCompleted.getSegmentNumber());
            }
            val iter = state.futureSegments.entrySet().iterator();
            while (iter.hasNext()) {
                Entry<Segment, Set<Integer>> entry = iter.next();
                if (entry.getValue().isEmpty()) {
                    state.unassignedSegments.put(entry.getKey(), 0L);
                    iter.remove();
                }
            }
        }
    }
    
    @RequiredArgsConstructor
    static class CheckpointReader extends ReaderGroupStateUpdate {
        private static final long serialVersionUID = 1L;
        private final String checkpointId;
        private final String readerId;
        private final Map<Segment, Long> positions; //Immutable
        
        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            state.checkpointState.readerCheckpointed(checkpointId, readerId, positions);
        }
    }
    
    @RequiredArgsConstructor
    static class CreateCheckpoint extends ReaderGroupStateUpdate {
        private static final long serialVersionUID = 1L;
        private final String checkpointId;
        
        CreateCheckpoint() {
            this(UUID.randomUUID().toString());
        }
        
        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            state.checkpointState.beginNewCheckpoint(checkpointId, state.getOnlineReaders(), state.getUnassignedSegments());
        }
    }
    
    @RequiredArgsConstructor
    static class ClearCheckpoints extends ReaderGroupStateUpdate {
        private static final long serialVersionUID = 1L;
        private final String clearUpThroughCheckpoint;
        
        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            state.checkpointState.clearCheckpointsThrough(clearUpThroughCheckpoint);
        }
    }


}
