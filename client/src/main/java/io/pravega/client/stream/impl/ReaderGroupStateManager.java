/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.stream.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderNotInReaderGroupException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.impl.ReaderGroupState.AcquireSegment;
import io.pravega.client.stream.impl.ReaderGroupState.AddReader;
import io.pravega.client.stream.impl.ReaderGroupState.CheckpointReader;
import io.pravega.client.stream.impl.ReaderGroupState.ClearCheckpointsBefore;
import io.pravega.client.stream.impl.ReaderGroupState.CreateCheckpoint;
import io.pravega.client.stream.impl.ReaderGroupState.UpdateCheckpointPublished;
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

import io.pravega.shared.NameUtils;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.client.stream.impl.ReaderGroupImpl.getEndSegmentsForStreams;
import static io.pravega.client.stream.impl.ReaderGroupImpl.getSegmentsForStreams;
import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;
import static io.pravega.common.concurrent.Futures.getThrowingException;

/**
 * Manages the state of the reader group on behalf of a reader.
 * 
 * {@link #initializeReader(long)} must be called upon reader startup before any other methods.
 * 
 * {@link #readerShutdown(Position)} should be called when the reader is shutting down. After this
 * method is called no other methods should be called on this class.
 * 
 * This class updates makes transitions using the {@link ReaderGroupState} object. If there are available
 * segments a reader can acquire them by calling {@link #acquireNewSegmentsIfNeeded(long, Position)}.
 * 
 * To balance load across multiple readers a reader can release segments so that other readers can acquire
 * them by calling {@link #releaseSegment(Segment, long, long, Position)}. A reader can tell if calling this method is
 * needed by calling {@link #findSegmentToReleaseIfRequired()}
 * 
 * Finally when a segment is sealed it may have one or more successors. So when a reader comes to the end of a
 * segment it should call {@link #handleEndOfSegment(SegmentWithRange)} so that it can continue reading from the
 * successor to that segment.
 */
@Slf4j
public class ReaderGroupStateManager {
    
    static final Duration TIME_UNIT = Duration.ofMillis(1000);
    static final Duration UPDATE_WINDOW = Duration.ofMillis(30000);
    static final Duration UPDATE_CONFIG_WINDOW = Duration.ofMillis(10000);
    private static final double COMPACTION_PROBABILITY = 0.05;
    private static final int MIN_BYTES_BETWEEN_COMPACTIONS = 512 * 1024;
    private final Object decisionLock = new Object();
    private final HashHelper hashHelper;
    @Getter
    private final String readerId;
    @Getter
    private final String scope;
    @Getter
    private final String groupName;
    private final StateSynchronizer<ReaderGroupState> sync;
    private final Controller controller;
    private final TimeoutTimer releaseTimer;
    private final TimeoutTimer acquireTimer;
    private final TimeoutTimer fetchStateTimer;
    private final TimeoutTimer checkpointTimer;
    private final TimeoutTimer lagUpdateTimer;
    private final TimeoutTimer updateConfigTimer;

    ReaderGroupStateManager(String scope, String groupName, String readerId, StateSynchronizer<ReaderGroupState> sync, Controller controller, Supplier<Long> nanoClock) {
        Preconditions.checkNotNull(readerId);
        Preconditions.checkNotNull(sync);
        Preconditions.checkNotNull(controller);
        this.readerId = readerId;
        this.scope = scope;
        this.groupName = groupName;
        this.hashHelper = HashHelper.seededWith(readerId);
        this.sync = sync;
        this.controller = controller;
        if (nanoClock == null) {
            nanoClock = System::nanoTime;
        }
        releaseTimer = new TimeoutTimer(TIME_UNIT, nanoClock);
        acquireTimer = new TimeoutTimer(Duration.ZERO, nanoClock);
        fetchStateTimer = new TimeoutTimer(Duration.ZERO, nanoClock);
        checkpointTimer = new TimeoutTimer(TIME_UNIT, nanoClock);
        lagUpdateTimer = new TimeoutTimer(TIME_UNIT, nanoClock);
        updateConfigTimer = new TimeoutTimer(TIME_UNIT, nanoClock);
    }

    /**
     * Add this reader to the reader group so that it is able to acquire segments
     */
    void initializeReader(long initialAllocationDelay) {
        boolean alreadyAdded = sync.updateState((state, updates) -> {
            if (state.getSegments(readerId) == null) {
                log.debug("Adding reader {} to reader group. CurrentState is: {}", readerId, state);
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
        sync.fetchUpdates();
        sync.updateState((state, updates) -> {
            Set<Segment> segments = state.getSegments(readerId);
            if (segments == null) {
                return;
            }
            log.debug("Removing reader {} from reader group. CurrentState is: {}. Position is: {}.", readerId, state, lastPosition);
            updates.add(new RemoveReader(readerId, lastPosition == null ? Collections.emptyMap()
                    : lastPosition.asImpl().getOwnedSegmentsWithOffsets()));
        });
    }
    
    void close() {
        sync.close();
    }

    /**
     * Handles a segment being completed by calling the controller to gather all successors to the
     * completed segment. To ensure consistent checkpoints, a segment cannot be released while a
     * checkpoint for the reader is pending, so it may or may not succeed.
     * 
     * @return true if the completed segment was released successfully.
     */
    boolean handleEndOfSegment(SegmentWithRange segmentCompleted) throws ReaderNotInReaderGroupException {
        final Map<SegmentWithRange, List<Long>> segmentToPredecessor;
        if (sync.getState().getEndSegments().containsKey(segmentCompleted.getSegment())) {
            segmentToPredecessor = Collections.emptyMap();
        } else {
            val successors = getAndHandleExceptions(controller.getSuccessors(segmentCompleted.getSegment()), RuntimeException::new);
            segmentToPredecessor = successors.getSegmentToPredecessor();
        }

        AtomicBoolean reinitRequired = new AtomicBoolean(false);
        boolean result = sync.updateState((state, updates) -> {
            if (!state.isReaderOnline(readerId)) {
                log.error("Reader " + readerId + " is offline according to the state but is attempting to update it.");
                reinitRequired.set(true);
                return false;
            } 
            if (!state.getSegments(readerId).contains(segmentCompleted.getSegment())) {
                log.error("Reader " + readerId + " is does not own the segment " + segmentCompleted + "but is attempting to release it.");
                reinitRequired.set(true);
                return false;
            }
            
            log.debug("Marking segment {} as completed in reader group. CurrentState is: {}", segmentCompleted, state);
            reinitRequired.set(false);
            //This check guards against another checkpoint having started.
            if (state.getCheckpointForReader(readerId) == null) {
                updates.add(new SegmentCompleted(readerId, segmentCompleted, segmentToPredecessor));
                return true;
            } 
            return false;
        });
        if (reinitRequired.get()) {
            throw new ReaderNotInReaderGroupException(readerId);
        }
        acquireTimer.zero();
        return result;
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

    boolean reachedEndOfStream() {
        fetchUpdatesIfNeeded();
        return sync.getState().isEndOfData();
    }

    /**
     * Releases a segment to another reader. This reader should no longer read from the segment. 
     * 
     * @param segment The segment to be released
     * @param lastOffset The offset from which the new owner should start reading from.
     * @param timeLag How far the reader is from the tail of the stream in time.
     * @param position The last read position
     * @return a boolean indicating if the segment was successfully released.
     * @throws ReaderNotInReaderGroupException If the reader has been declared offline.
     */
    boolean releaseSegment(Segment segment, long lastOffset, long timeLag, Position position) throws ReaderNotInReaderGroupException {
        sync.updateState((state, updates) -> {
            Set<Segment> segments = state.getSegments(readerId);
            if (segments != null && segments.contains(segment) && state.getCheckpointForReader(readerId) == null
                    && doesReaderOwnTooManySegments(state)) {
                updates.add(new ReleaseSegment(readerId, segment, lastOffset));
                updates.add(new UpdateDistanceToTail(readerId, timeLag, position.asImpl().getOwnedSegmentRangesWithOffsets()));
            }
        });
        ReaderGroupState state = sync.getState();
        releaseTimer.reset(calculateReleaseTime(readerId, state));
        acquireTimer.reset(calculateAcquireTime(readerId, state));
        resetLagUpdateTimer();
        if (!state.isReaderOnline(readerId)) {
            throw new ReaderNotInReaderGroupException(readerId);
        }
        return !state.getSegments(readerId).contains(segment);
    }

    @VisibleForTesting
    static Duration calculateReleaseTime(String readerId, ReaderGroupState state) {
        return TIME_UNIT.multipliedBy(1 + state.getRanking(readerId));
    }

    /**
     * If there are unassigned segments and this host has not acquired one in a while, acquires them.
     * @param timeLag the time between the reader's current location and the end of the stream
     * @param position the last position read by the reader.
     * @return A map from the new segment that was acquired to the offset to begin reading from within the segment.
     */
    Map<SegmentWithRange, Long> acquireNewSegmentsIfNeeded(long timeLag, Position position) throws ReaderNotInReaderGroupException {
        fetchUpdatesIfNeeded();
        if (shouldAcquireSegment()) {
            return acquireSegment(timeLag, position);
        } else {
            return Collections.emptyMap();
        }
    }

    boolean canUpdateLagIfNeeded() {
        return !fetchStateTimer.hasRemaining();
    }
    
    boolean updateLagIfNeeded(long timeLag, Position position) {
        if (!lagUpdateTimer.hasRemaining()) {
            log.debug("Update lag for reader {}", readerId);
            resetLagUpdateTimer();
            sync.updateStateUnconditionally(new UpdateDistanceToTail(readerId, timeLag, position.asImpl().getOwnedSegmentRangesWithOffsets()));
            resetFetchUpdateTimer();
            sync.fetchUpdates();
            return true;
        }
        return false;
    }

    private void resetFetchUpdateTimer() {
        long groupRefreshTimeMillis = sync.getState().getConfig().getGroupRefreshTimeMillis();
        fetchStateTimer.reset(Duration.ofMillis(groupRefreshTimeMillis));
    }

    private void resetLagUpdateTimer() {
        lagUpdateTimer.reset(TIME_UNIT.multipliedBy(sync.getState().getOnlineReaders().size() + 1));
    }

    private void fetchUpdatesIfNeeded() {
        if (!fetchStateTimer.hasRemaining()) {
            log.debug("Update group state for reader {}", readerId);
            sync.fetchUpdates();
            resetFetchUpdateTimer();
            compactIfNeeded();
        }
    }

    void updateConfigIfNeeded() {
        if (!updateConfigTimer.hasRemaining()) {
            fetchUpdatesIfNeeded();
            ReaderGroupState state = sync.getState();
            if (state.isUpdatingConfig()) {
                ReaderGroupConfig controllerConfig = getThrowingException(controller.getReaderGroupConfig(scope, groupName));
                log.debug("Updating the readergroup {} with the new config {} obtained from the controller", groupName, controllerConfig);
                Map<SegmentWithRange, Long> segments = getSegmentsForStreams(controller, controllerConfig);
                sync.updateState((s, updates) -> {
                    if (s.getConfig().getGeneration() < controllerConfig.getGeneration()) {
                        updates.add(new ReaderGroupState.ReaderGroupStateInit(controllerConfig, segments, getEndSegmentsForStreams(controllerConfig), false));
                    }
                });
            }
            updateConfigTimer.reset(UPDATE_CONFIG_WINDOW);
        }
    }
    
    private void compactIfNeeded() {
        //Make sure it has been a while, and compaction are staggered.
        if (sync.bytesWrittenSinceCompaction() > MIN_BYTES_BETWEEN_COMPACTIONS && Math.random() < COMPACTION_PROBABILITY) {
            log.debug("Compacting reader group state {}", sync.getState());
            sync.compact(ReaderGroupState.CompactReaderGroupState::new);
        }
    }

    boolean canAcquireSegmentIfNeeded() {
        return !acquireTimer.hasRemaining();
    }

    boolean shouldAcquireSegment() throws ReaderNotInReaderGroupException {
        synchronized (decisionLock) {
            if (acquireTimer.hasRemaining()) {
                return false;
            }
            ReaderGroupState state = sync.getState();
            if (!state.isReaderOnline(readerId)) {
                throw new ReaderNotInReaderGroupException(readerId);
            }
            if (state.getCheckpointForReader(readerId) != null) {
                return false;
            }
            if (state.getNumberOfUnassignedSegments() == 0) {
                acquireTimer.reset(calculateAcquireTime(readerId, state));
                return false;
            }
            acquireTimer.reset(UPDATE_WINDOW);
            releaseTimer.reset(UPDATE_WINDOW);
            return true;
        }
    }

    private Map<SegmentWithRange, Long> acquireSegment(long timeLag, Position position) throws ReaderNotInReaderGroupException {
        AtomicBoolean reinitRequired = new AtomicBoolean();
        Map<SegmentWithRange, Long> result = sync.updateState((state, updates) -> {
            if (!state.isReaderOnline(readerId)) {
                reinitRequired.set(true);
                return Collections.<SegmentWithRange, Long>emptyMap();
            }
            reinitRequired.set(false);
            if (state.getCheckpointForReader(readerId) != null) {
                return Collections.<SegmentWithRange, Long>emptyMap();
            }
            int toAcquire = calculateNumSegmentsToAcquire(state);
            if (toAcquire == 0) {
                return Collections.<SegmentWithRange, Long>emptyMap();
            }
            Map<SegmentWithRange, Long> unassignedSegments = state.getUnassignedSegments();
            Map<SegmentWithRange, Long> acquired = new HashMap<>(toAcquire);
            Iterator<Entry<SegmentWithRange, Long>> iter = unassignedSegments.entrySet().iterator();
            for (int i = 0; i < toAcquire; i++) {
                assert iter.hasNext();
                Entry<SegmentWithRange, Long> segment = iter.next();
                acquired.put(segment.getKey(), segment.getValue());
                updates.add(new AcquireSegment(readerId, segment.getKey().getSegment()));
            }
            updates.add(new UpdateDistanceToTail(readerId, timeLag, position.asImpl().getOwnedSegmentRangesWithOffsets()));
            return acquired;
        });
        if (reinitRequired.get()) {
            throw new ReaderNotInReaderGroupException(readerId);
        }
        releaseTimer.reset(calculateReleaseTime(readerId, sync.getState()));
        acquireTimer.reset(calculateAcquireTime(readerId, sync.getState()));
        resetLagUpdateTimer();
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
        int multiplier = state.getNumberOfReaders() - state.getRanking(readerId);
        Preconditions.checkArgument(multiplier >= 1, "Invalid acquire timer multiplier");
        return TIME_UNIT.multipliedBy(multiplier);
    }

    /**
     * Returns name of the checkpoint to be processed by this reader
     * @return String - name of the first checkpoint pending processing for this Reader.
     * */
    String getCheckpoint() throws ReaderNotInReaderGroupException {
        fetchUpdatesIfNeeded();
        ReaderGroupState state = sync.getState();
        long automaticCpInterval = state.getConfig().getAutomaticCheckpointIntervalMillis();
        if (!state.isReaderOnline(readerId)) {
            throw new ReaderNotInReaderGroupException(readerId);
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

    void checkpoint(String checkpointName, PositionInternal lastPosition) throws ReaderNotInReaderGroupException {
        AtomicBoolean reinitRequired = new AtomicBoolean(false);
        sync.updateState((state, updates) -> {
            if (!state.isReaderOnline(readerId)) {
                reinitRequired.set(true);
            } else {
                reinitRequired.set(false);
                String cpName = state.getCheckpointForReader(readerId);
                if (checkpointName.equals(cpName)) {
                    updates.add(new CheckpointReader(checkpointName, readerId, lastPosition.getOwnedSegmentsWithOffsets()));
                } else {
                    log.warn("{} was asked to checkpoint for {} but the state says its next checkpoint should be {}", readerId, checkpointName, cpName);
                }
            }
        });
        if (reinitRequired.get()) {
            throw new ReaderNotInReaderGroupException(readerId);
        }
    }

    boolean isCheckpointSilent(String atCheckpoint) {
        return sync.getState().getCheckpointState().isCheckpointSilent(atCheckpoint);
    }

    @SneakyThrows(CheckpointFailedException.class)
    void updateTruncationStreamCutIfNeeded() {
        ReaderGroupState state = sync.getState();
        // here add check if RG is Subscriber and autoPublishTruncationStreamCut = true
        if (state.getConfig().getRetentionType().equals(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
        && !state.getCheckpointState().isLastCheckpointPublished()) {
            // we get here only when this is the reader that has completed the lastCheckpoint
            Optional<Map<Stream, Map<Segment, Long>>> cuts = state.getPositionsForLastCompletedCheckpoint();
            cuts.orElseThrow(() -> new CheckpointFailedException("Could not get positions for last checkpoint."))
                 .entrySet().forEach(entry ->
                        controller.updateSubscriberStreamCut(entry.getKey().getScope(), entry.getKey().getStreamName(),
                                NameUtils.getScopedReaderGroupName(scope, groupName), state.getConfig().getReaderGroupId(),
                                state.getConfig().getGeneration(), new StreamCutImpl(entry.getKey(), entry.getValue())));
                sync.updateStateUnconditionally(new UpdateCheckpointPublished(true));
        }
    }

    Set<Stream> getStreams() {
        return Collections.unmodifiableSet(sync.getState().getConfig().getStartingStreamCuts().keySet());
    }
    
    Map<SegmentWithRange, Long> getLastReadpositions(Stream stream) {
        return sync.getState().getLastReadPositions(stream);
    }
}
