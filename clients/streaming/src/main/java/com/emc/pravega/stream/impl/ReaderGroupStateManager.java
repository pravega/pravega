/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.hash.HashHelper;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.ReaderGroupState.AddReader;
import com.emc.pravega.stream.impl.ReaderGroupState.AcquireSegment;
import com.emc.pravega.stream.impl.ReaderGroupState.ReaderGroupStateUpdate;
import com.emc.pravega.stream.impl.ReaderGroupState.ReleaseSegment;
import com.emc.pravega.stream.impl.ReaderGroupState.RemoveReader;
import com.emc.pravega.stream.impl.ReaderGroupState.SegmentCompleted;
import com.emc.pravega.stream.impl.ReaderGroupState.UpdateDistanceToTail;
import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import lombok.val;

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
public class ReaderGroupStateManager {
    
    static final Duration TIME_UNIT = Duration.ofMillis(1000);
    static final Duration UPDATE_TIME = Duration.ofMillis(30000);
    private final Object decisionLock = new Object();
    private final HashHelper hashHelper;
    private final String readerId;
    private final StateSynchronizer<ReaderGroupState> sync;
    private final Controller controller;
    private final TimeoutTimer releaseTimer;
    private final TimeoutTimer acquireTimer;

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
    }
    
    static void initializeReaderGroup(StateSynchronizer<ReaderGroupState> sync,
            ReaderGroupConfig config, Map<Segment, Long> segments) {
        sync.initialize(new ReaderGroupState.ReaderGroupStateInit(config, segments));
    }
    
    /**
     * Add this reader to the reader group so that it is able to acquire segments
     */
    void initializeReader() {
        AtomicBoolean alreadyAdded = new AtomicBoolean(false);
        sync.updateState(state -> {
            if (state.getSegments(readerId) == null) {
                return Collections.singletonList(new AddReader(readerId));
            } else {
                alreadyAdded.set(true);
                return null;
            }
        });
        if (alreadyAdded.get()) {
            throw new IllegalStateException("The requested reader: " + readerId
                    + " cannot be added to the group because it is already in the group. Perhaps close() was not called?");
        }
        acquireTimer.zero();
    }
    
    /**
     * Shuts down a reader, releasing all of its segments. The reader should cease all operations.
     * @param lastPosition The last position the reader successfully read from.
     */
    void readerShutdown(PositionInternal lastPosition) {
        readerShutdown(readerId, lastPosition, sync);
    }
    
    /**
     * Shuts down a reader, releasing all of its segments. The reader should cease all operations.
     * @param lastPosition The last position the reader successfully read from.
     */
    static void readerShutdown(String readerId, PositionInternal lastPosition, StateSynchronizer<ReaderGroupState> sync) {
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
        val successors = getAndHandleExceptions(controller.getSuccessors(segmentCompleted),
                                                RuntimeException::new);
        sync.updateState(state -> {
            return Collections.singletonList(new SegmentCompleted(readerId, segmentCompleted, successors));
        });
        acquireTimer.zero();
    }

    /**
     * If a segment should be released because the distribution of segments is imbalanced and
     * this reader has not done so in a while, this returns the segment that should be released.
     */
    Segment findSegmentToReleaseIfRequired() {
        if (!releaseTimer.hasRemaining()) {
            sync.fetchUpdates();
        }
        Segment segment = null;
        synchronized (decisionLock) {
            if (!releaseTimer.hasRemaining() && doesReaderOwnTooManySegments(sync.getState())) {
                segment = findSegmentToRelease();
                if (segment != null) {
                    releaseTimer.reset(UPDATE_TIME);
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
     * Releases a segment to another reader. This reader should no longer read from the segment. 
     * 
     * @param segment The segment to be released
     * @param lastOffset The offset from which the new owner should start reading from.
     * @param timeLag How far the reader is from the tail of the stream in time.
     * @return a boolean indicating if the segment was successfully released.
     */
    boolean releaseSegment(Segment segment, long lastOffset, long timeLag) {
        sync.updateState(state -> {
            Set<Segment> segments = state.getSegments(readerId);
            if (segments == null || !segments.contains(segment) || !doesReaderOwnTooManySegments(state)) {
                return null;
            }
            List<ReaderGroupStateUpdate> result = new ArrayList<>(2);
            result.add(new ReleaseSegment(readerId, segment, lastOffset));
            result.add(new UpdateDistanceToTail(readerId, timeLag));
            return result;
        });
        ReaderGroupState state = sync.getState();
        releaseTimer.reset(calculateReleaseTime(state));
        return !state.getSegments(readerId).contains(segment);
    }

    private Duration calculateReleaseTime(ReaderGroupState state) {
        return TIME_UNIT.multipliedBy(1 + state.getRanking(readerId));
    }

    /**
     * If there are unassigned segments and this host has not acquired one in a while, acquires them.
     * @return A map from the new segment that was acquired to the offset to begin reading from within the segment.
     */
    Map<Segment, Long> acquireNewSegmentsIfNeeded(long timeLag) {
        if (!acquireTimer.hasRemaining()) {
            sync.fetchUpdates();
        }
        if (shouldAcquireSegment()) {
            return acquireSegment(timeLag);
        } else {
            return Collections.emptyMap();
        }
    }
    
    private boolean shouldAcquireSegment() {
        synchronized (decisionLock) {
            if (acquireTimer.hasRemaining()) {
                return false;
            }
            if (sync.getState().getNumberOfUnassignedSegments() == 0) {
                return false;
            }
            acquireTimer.reset(UPDATE_TIME);
            return true;
        }
    }

    private Map<Segment, Long> acquireSegment(long timeLag) {
        AtomicReference<Map<Segment, Long>> result = new AtomicReference<>();
        sync.updateState(state -> {
            int toAcquire = calculateNumSegmentsToAcquire(state);
            if (toAcquire == 0) {
                result.set(Collections.emptyMap());
                return null;
            }
            Map<Segment, Long> unassignedSegments = state.getUnassignedSegments();
            Map<Segment, Long> acquired = new HashMap<>(toAcquire);
            List<ReaderGroupStateUpdate> updates = new ArrayList<>(toAcquire);
            Iterator<Entry<Segment, Long>> iter = unassignedSegments.entrySet().iterator();
            for (int i = 0; i < toAcquire; i++) {
                assert iter.hasNext();
                Entry<Segment, Long> segment = iter.next();
                acquired.put(segment.getKey(), segment.getValue());
                updates.add(new AcquireSegment(readerId, segment.getKey()));
            }
            updates.add(new UpdateDistanceToTail(readerId, timeLag));
            result.set(acquired);
            return updates;
        });
        acquireTimer.reset(calculateAcquireTime(sync.getState()));
        return result.get();
    }
    
    private int calculateNumSegmentsToAcquire(ReaderGroupState state) {
        int unassignedSegments = state.getNumberOfUnassignedSegments();
        if (unassignedSegments == 0) {
            return 0;
        }
        int numSegments = state.getNumberOfSegments();
        int segmentsOwned = state.getSegments(readerId).size();
        int numReaders = state.getNumberOfReaders();
        return Math.max(Math.max(
                                 1,
                                 Math.round(numSegments / (float) numReaders) - segmentsOwned),
                                 unassignedSegments / numReaders);
    }

    private Duration calculateAcquireTime(ReaderGroupState state) {
        return TIME_UNIT.multipliedBy(state.getNumberOfReaders() - state.getRanking(readerId));
    }
}
