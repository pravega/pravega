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
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.impl.ReaderGroupState.AcquireSegment;
import io.pravega.client.stream.impl.ReaderGroupState.AddReader;
import io.pravega.client.stream.impl.ReaderGroupState.CheckpointReader;
import io.pravega.client.stream.impl.ReaderGroupState.ClearCheckpointsBefore;
import io.pravega.client.stream.impl.ReaderGroupState.CreateCheckpoint;
import io.pravega.client.stream.impl.ReaderGroupState.ReleaseSegment;
import io.pravega.client.stream.impl.ReaderGroupState.RemoveReader;
import io.pravega.client.stream.impl.ReaderGroupState.SegmentCompleted;
import io.pravega.client.stream.impl.ReaderGroupState.UpdateDistanceToTail;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.hash.HashHelper;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;

/**
 * Manages the state of the reader group on behalf of a reader.
 * 
 * {@link #initializeReader()} must be called upon reader startup before any other methods.
 * 
 * {@link #readerShutdown(PositionInternal)} should be called when the reader is shutting down. After this
 * method is called no other methods should be called on this class.
 * 
 * This class updates makes transitions using the {@link ReaderGroupState} object. If there are available
 * segments a reader can acquire them by calling {@link #acquireNewSegmentsIfNeeded(long)}.
 * 
 * To balance load across multiple readers a reader can release segments so that other readers can acquire
 * them by calling {@link #releaseSegment(Segment, long, long)}. A reader can tell if calling this method is
 * needed by calling {@link #findSegmentToReleaseIfRequired()}
 * 
 * Finally when a segment is sealed it may have one or more successors. So when a reader comes to the end of a
 * segment it should call {@link #handleEndOfSegment(Segment)} so that it can continue reading from the
 * successor to that segment.
 */
@Slf4j
public class ReaderGroupStateManager {
    
    static final Duration TIME_UNIT = Duration.ofMillis(1000);
    static final Duration UPDATE_WINDOW = Duration.ofMillis(30000);
    private static final double COMPACTION_PROBABILITY = 0.05;
    private static final int MIN_BYTES_BETWEEN_COMPACTIONS = 512 * 1024;
    private final Object decisionLock = new Object();
    private final HashHelper hashHelper;
    @Getter
    private final String readerId;
    private final StateSynchronizer<ReaderGroupState> sync;
    private final Controller controller;
    private final TimeoutTimer releaseTimer;
    private final TimeoutTimer acquireTimer;
    private final TimeoutTimer fetchStateTimer;
    private final TimeoutTimer checkpointTimer;

    ReaderGroupStateManager(String readerId, StateSynchronizer<ReaderGroupState> sync, Controller controller, Supplier<Long> nanoClock) {
        Preconditions.checkNotNull(readerId);
        Preconditions.checkNotNull(sync);
        Preconditions.checkNotNull(controller);
        this.readerId = readerId;
        this.hashHelper = HashHelper.seededWith(readerId);
        this.sync = sync;
        this.controller = controller;
        if (nanoClock == null) {
            nanoClock = System::nanoTime;
        }
        releaseTimer = new TimeoutTimer(TIME_UNIT, nanoClock);
        acquireTimer = new TimeoutTimer(TIME_UNIT, nanoClock);
        fetchStateTimer = new TimeoutTimer(TIME_UNIT, nanoClock);
        checkpointTimer = new TimeoutTimer(TIME_UNIT, nanoClock);
    }

    /**
     * Add this reader to the reader group so that it is able to acquire segments
     */
    void initializeReader(long initialAllocationDelay) {
        boolean alreadyAdded = sync.updateState((state, updates) -> {
            if (state.getSegments(readerId) == null) {
                log.debug("Adding reader {} to reader grop. CurrentState is: {}", readerId, state);
                updates.add(new AddReader(readerId));
                return false;
            } else {
                return true;
            }
        });
        if (alreadyAdded) {
            throw new IllegalStateException("The requested reader: " + readerId
                    + " cannot be added to the group because it is already in the group. Perhaps close() was not called?");
        }
        long randomDelay = (long) (Math.random() * Math.min(initialAllocationDelay, sync.getState().getConfig().getGroupRefreshTimeMillis()));
        acquireTimer.reset(Duration.ofMillis(initialAllocationDelay + randomDelay));
    }
    
    /**
     * Shuts down a reader, releasing all of its segments. The reader should cease all operations.
     * @param lastPosition The last position the reader successfully read from.
     */
    void readerShutdown(Position lastPosition) {
        readerShutdown(readerId, lastPosition, sync);
    }
    
    /**
     * Shuts down a reader, releasing all of its segments. The reader should cease all operations.
     * @param lastPosition The last position the reader successfully read from.
     */
    static void readerShutdown(String readerId, Position lastPosition, StateSynchronizer<ReaderGroupState> sync) {
        sync.updateState((state, updates) -> {
            Set<Segment> segments = state.getSegments(readerId);
            if (segments == null) {
                return;
            }
            log.debug("Removing reader {} from reader grop. CurrentState is: {}", readerId, state);
            if (lastPosition != null && !lastPosition.asImpl().getOwnedSegments().containsAll(segments)) {
                throw new IllegalArgumentException(
                        "When shutting down a reader: Given position does not match the segments it was assigned: \n"
                                + segments + " \n vs \n " + lastPosition.asImpl().getOwnedSegments());
            }
            updates.add(new RemoveReader(readerId, lastPosition == null ? null : lastPosition.asImpl().getOwnedSegmentsWithOffsets()));
        });
    }
    
    void close() {
        sync.close();
    }

    /**
     * Handles a segment being completed by calling the controller to gather all successors to the completed segment.
     */
    void handleEndOfSegment(Segment segmentCompleted, boolean fetchSuccesors) throws ReinitializationRequiredException {
        final Map<Segment, List<Long>> segmentToPredecessor;
        if (fetchSuccesors) {
            val successors = getAndHandleExceptions(controller.getSuccessors(segmentCompleted), RuntimeException::new);
            segmentToPredecessor = successors.getSegmentToPredecessor();
        } else {
            segmentToPredecessor = Collections.emptyMap();
        }

        AtomicBoolean reinitRequired = new AtomicBoolean(false);
        sync.updateState((state, updates) -> {
            if (!state.isReaderOnline(readerId)) {
                reinitRequired.set(true);
            } else {
                log.debug("Marking segment {} as completed in reader group. CurrentState is: {}", segmentCompleted, state);
                reinitRequired.set(false);
                updates.add(new SegmentCompleted(readerId, segmentCompleted, segmentToPredecessor));
            }
        });
        if (reinitRequired.get()) {
            throw new ReinitializationRequiredException();
        }
        acquireTimer.zero();
    }

    /**
     * If a segment should be released because the distribution of segments is imbalanced and
     * this reader has not done so in a while, this returns the segment that should be released.
     */
    Segment findSegmentToReleaseIfRequired() {
        fetchUpdatesIfNeeded();
        Segment segment = null;
        synchronized (decisionLock) {
            if (!releaseTimer.hasRemaining() && sync.getState().getCheckpointForReader(readerId) == null
                    && doesReaderOwnTooManySegments(sync.getState())) {
                segment = findSegmentToRelease();
                if (segment != null) {
                    releaseTimer.reset(UPDATE_WINDOW);
                    acquireTimer.reset(UPDATE_WINDOW);
                }
            }
        }
        return segment;
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
        return sizesOfAssignemnts.get(readerId) > min + Math.max(1, state.getNumberOfUnassignedSegments());
    }

    /**
     * Given a set of segments returns one to release. The one returned is arbitrary.
     */
    private Segment findSegmentToRelease() {
        Set<Segment> segments = sync.getState().getSegments(readerId);
        return segments.stream()
                       .max((s1, s2) -> Double.compare(hashHelper.hashToRange(s1.getScopedName()),
                                                       hashHelper.hashToRange(s2.getScopedName())))
                       .orElse(null);
    }

    /**
     * Fetch the configured end offset for a configured segment. If end offset is not configured return Long.MAX_VALUE.
     *
     * @param segment Segment.
     * @return endOffset of the segment.
     */
    long getEndOffsetForSegment(Segment segment) {
        return Optional.ofNullable(sync.getState().getEndSegments().get(segment)).orElse(Long.MAX_VALUE);
    }

    /**
     * Releases a segment to another reader. This reader should no longer read from the segment. 
     * 
     * @param segment The segment to be released
     * @param lastOffset The offset from which the new owner should start reading from.
     * @param timeLag How far the reader is from the tail of the stream in time.
     * @return a boolean indicating if the segment was successfully released.
     * @throws ReinitializationRequiredException If the reader has been declared offline.
     */
    boolean releaseSegment(Segment segment, long lastOffset, long timeLag) throws ReinitializationRequiredException {
        sync.updateState((state, updates) -> {
            Set<Segment> segments = state.getSegments(readerId);
            if (segments != null && segments.contains(segment) && state.getCheckpointForReader(readerId) == null
                    && doesReaderOwnTooManySegments(state)) {
                updates.add(new ReleaseSegment(readerId, segment, lastOffset));
                updates.add(new UpdateDistanceToTail(readerId, timeLag));
            }
        });
        ReaderGroupState state = sync.getState();
        releaseTimer.reset(calculateReleaseTime(readerId, state));
        acquireTimer.reset(calculateAcquireTime(readerId, state));
        if (!state.isReaderOnline(readerId)) {
            throw new ReinitializationRequiredException();
        }
        return !state.getSegments(readerId).contains(segment);
    }

    @VisibleForTesting
    static Duration calculateReleaseTime(String readerId, ReaderGroupState state) {
        return TIME_UNIT.multipliedBy(1 + state.getRanking(readerId));
    }

    /**
     * If there are unassigned segments and this host has not acquired one in a while, acquires them.
     * @return A map from the new segment that was acquired to the offset to begin reading from within the segment.
     */
    Map<Segment, Long> acquireNewSegmentsIfNeeded(long timeLag) throws ReinitializationRequiredException {
        fetchUpdatesIfNeeded();
        if (shouldAcquireSegment()) {
            return acquireSegment(timeLag);
        } else {
            return Collections.emptyMap();
        }
    }

    private void fetchUpdatesIfNeeded() {
        if (!fetchStateTimer.hasRemaining()) {
            sync.fetchUpdates();
            long groupRefreshTimeMillis = sync.getState().getConfig().getGroupRefreshTimeMillis();
            fetchStateTimer.reset(Duration.ofMillis(groupRefreshTimeMillis));
            compactIfNeeded();
        }
    }
    
    private void compactIfNeeded() {
        //Make sure it has been a while, and compaction are staggered.
        if (sync.bytesWrittenSinceCompaction() > MIN_BYTES_BETWEEN_COMPACTIONS && Math.random() < COMPACTION_PROBABILITY) {
            log.debug("Compacting reader group state {}", sync.getState());
            sync.compact(s -> new ReaderGroupState.CompactReaderGroupState(s));
        }
    }
    
    private boolean shouldAcquireSegment() throws ReinitializationRequiredException {
        synchronized (decisionLock) {
            ReaderGroupState state = sync.getState();
            if (!state.isReaderOnline(readerId)) {
                throw new ReinitializationRequiredException();
            }
            if (acquireTimer.hasRemaining()) {
                return false;
            }
            if (state.getCheckpointForReader(readerId) != null) {
                return false;
            }
            if (state.getNumberOfUnassignedSegments() == 0) {
                if (doesReaderOwnTooManySegments(state)) {
                    acquireTimer.reset(calculateAcquireTime(readerId, state));
                }
                return false;
            }
            acquireTimer.reset(UPDATE_WINDOW);
            releaseTimer.reset(UPDATE_WINDOW);
            return true;
        }
    }

    private Map<Segment, Long> acquireSegment(long timeLag) throws ReinitializationRequiredException {
        AtomicBoolean reinitRequired = new AtomicBoolean();
        Map<Segment, Long> result = sync.updateState((state, updates) -> {
            if (!state.isReaderOnline(readerId)) {
                reinitRequired.set(true);
                return Collections.<Segment, Long>emptyMap();
            }
            reinitRequired.set(false);
            if (state.getCheckpointForReader(readerId) != null) {
                return Collections.<Segment, Long>emptyMap();
            }
            int toAcquire = calculateNumSegmentsToAcquire(state);
            if (toAcquire == 0) {
                return Collections.<Segment, Long>emptyMap();
            }
            Map<Segment, Long> unassignedSegments = state.getUnassignedSegments();
            Map<Segment, Long> acquired = new HashMap<>(toAcquire);
            Iterator<Entry<Segment, Long>> iter = unassignedSegments.entrySet().iterator();
            for (int i = 0; i < toAcquire; i++) {
                assert iter.hasNext();
                Entry<Segment, Long> segment = iter.next();
                acquired.put(segment.getKey(), segment.getValue());
                updates.add(new AcquireSegment(readerId, segment.getKey()));
            }
            updates.add(new UpdateDistanceToTail(readerId, timeLag));
            return acquired;
        });
        if (reinitRequired.get()) {
            throw new ReinitializationRequiredException();
        }
        releaseTimer.reset(calculateReleaseTime(readerId, sync.getState()));
        acquireTimer.reset(calculateAcquireTime(readerId, sync.getState()));
        return result;
    }
    
    private int calculateNumSegmentsToAcquire(ReaderGroupState state) {
        int unassignedSegments = state.getNumberOfUnassignedSegments();
        if (unassignedSegments == 0) {
            return 0;
        }
        int numSegments = state.getNumberOfSegments();
        int segmentsOwned = state.getSegments(readerId).size();
        int numReaders = state.getNumberOfReaders();
        int equallyDistributed = unassignedSegments / numReaders;
        int fairlyDistributed = Math.min(unassignedSegments, Math.round(numSegments / (float) numReaders) - segmentsOwned);
        return Math.max(Math.max(equallyDistributed, fairlyDistributed), 1);
    }

    @VisibleForTesting
    static Duration calculateAcquireTime(String readerId, ReaderGroupState state) {
        return TIME_UNIT.multipliedBy(state.getNumberOfReaders() - state.getRanking(readerId));
    }
    
    String getCheckpoint() throws ReinitializationRequiredException {
        fetchUpdatesIfNeeded();
        ReaderGroupState state = sync.getState();
        long automaticCpInterval = state.getConfig().getAutomaticCheckpointIntervalMillis();
        if (!state.isReaderOnline(readerId)) {
            throw new ReinitializationRequiredException();
        }
        String checkpoint = state.getCheckpointForReader(readerId);
        if (checkpoint != null) {
            checkpointTimer.reset(Duration.ofMillis(automaticCpInterval));
            return checkpoint;
        }
        if (automaticCpInterval <= 0 || checkpointTimer.hasRemaining() || state.hasOngoingCheckpoint()) {
            return null;
        }
        sync.updateState((s, u) -> {
            if (!s.hasOngoingCheckpoint()) {
                CreateCheckpoint newCp = new CreateCheckpoint();
                u.add(newCp);
                u.add(new ClearCheckpointsBefore(newCp.getCheckpointId()));
                log.debug("Created new checkpoint: {} currentState is: {}", newCp, s);
            }
        });
        checkpointTimer.reset(Duration.ofMillis(automaticCpInterval));
        return state.getCheckpointForReader(readerId);
    }
    
    
    void checkpoint(String checkpointName, PositionInternal lastPosition) throws ReinitializationRequiredException {
        AtomicBoolean reinitRequired = new AtomicBoolean(false);
        sync.updateState((state, updates) -> {
            if (!state.isReaderOnline(readerId)) {
                reinitRequired.set(true);
            } else {
                reinitRequired.set(false);
                updates.add(new CheckpointReader(checkpointName, readerId, lastPosition.getOwnedSegmentsWithOffsets()));
            }
        });
        if (reinitRequired.get()) {
            throw new ReinitializationRequiredException();
        }
    }

    public String getOrRefreshDelegationTokenFor(Segment segmentId) {
            return getAndHandleExceptions(controller.getOrRefreshDelegationTokenFor(segmentId.getScope(), segmentId.getStreamName()), RuntimeException::new);
    }

}
