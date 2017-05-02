/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.server.containers;

import io.pravega.common.Exceptions;
import io.pravega.service.server.ContainerMetadata;
import io.pravega.service.server.EvictableMetadata;
import io.pravega.service.server.SegmentMetadata;
import io.pravega.service.server.UpdateableContainerMetadata;
import io.pravega.service.server.UpdateableSegmentMetadata;
import io.pravega.service.server.logs.operations.Operation;
import io.pravega.service.storage.LogAddress;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import lombok.extern.slf4j.Slf4j;

/**
 * Metadata for a Stream Segment Container.
 */
@Slf4j
@VisibleForTesting
@ThreadSafe
public class StreamSegmentContainerMetadata implements UpdateableContainerMetadata, EvictableMetadata {
    //region Members

    private static final long NO_EPOCH = Long.MIN_VALUE;

    private final String traceObjectId;
    private final AtomicLong sequenceNumber;
    private final AtomicLong lastTruncatedSequenceNumber;
    private final AtomicLong epoch;
    @GuardedBy("lock")
    private final HashMap<String, StreamSegmentMetadata> metadataByName;
    @GuardedBy("lock")
    private final HashMap<Long, StreamSegmentMetadata> metadataById;
    private final AtomicBoolean recoveryMode;
    private final int streamSegmentContainerId;
    private final int maxActiveSegmentCount;
    @GuardedBy("truncationMarkers")
    private final TreeMap<Long, LogAddress> truncationMarkers;
    @GuardedBy("truncationMarkers")
    private final TreeSet<Long> truncationPoints;
    private final Object lock = new Object();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentContainerMetadata.
     *
     * @param streamSegmentContainerId The Id of the StreamSegmentContainer.
     * @param maxActiveSegmentCount    The maximum number of segments that can be registered in this metadata at any given time.
     */
    public StreamSegmentContainerMetadata(int streamSegmentContainerId, int maxActiveSegmentCount) {
        Preconditions.checkArgument(maxActiveSegmentCount > 0, "maxActiveSegmentCount must be a positive integer.");
        this.traceObjectId = String.format("SegmentContainer[%d]", streamSegmentContainerId);
        this.streamSegmentContainerId = streamSegmentContainerId;
        this.maxActiveSegmentCount = maxActiveSegmentCount;
        this.sequenceNumber = new AtomicLong();
        this.metadataByName = new HashMap<>();
        this.metadataById = new HashMap<>();
        this.truncationMarkers = new TreeMap<>();
        this.truncationPoints = new TreeSet<>();
        this.recoveryMode = new AtomicBoolean();
        this.lastTruncatedSequenceNumber = new AtomicLong();
        this.epoch = new AtomicLong(NO_EPOCH);
    }

    //endregion

    //region SegmentMetadataCollection Implementation

    @Override
    public long getStreamSegmentId(String streamSegmentName, boolean updateLastUsed) {
        synchronized (this.lock) {
            StreamSegmentMetadata metadata = this.metadataByName.getOrDefault(streamSegmentName, null);
            if (updateLastUsed && metadata != null) {
                metadata.setLastUsed(getOperationSequenceNumber());
            }

            return metadata != null ? metadata.getId() : NO_STREAM_SEGMENT_ID;
        }
    }

    @Override
    public UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) {
        synchronized (this.lock) {
            return this.metadataById.getOrDefault(streamSegmentId, null);
        }
    }

    //endregion

    //region ContainerMetadata Implementation

    @Override
    public int getContainerId() {
        return this.streamSegmentContainerId;
    }

    @Override
    public long getContainerEpoch() {
        return this.epoch.get();
    }

    @Override
    public boolean isRecoveryMode() {
        return this.recoveryMode.get();
    }

    @Override
    public long getOperationSequenceNumber() {
        return this.sequenceNumber.get();
    }
    //endregion

    //region UpdateableContainerMetadata

    @Override
    public int getMaximumActiveSegmentCount() {
        return this.maxActiveSegmentCount;
    }

    @Override
    public int getActiveSegmentCount() {
        synchronized (this.lock) {
            return this.metadataById.size();
        }
    }

    @Override
    public UpdateableSegmentMetadata mapStreamSegmentId(String streamSegmentName, long streamSegmentId) {
        StreamSegmentMetadata segmentMetadata;
        synchronized (this.lock) {
            validateNewMapping(streamSegmentName, streamSegmentId);
            segmentMetadata = new StreamSegmentMetadata(streamSegmentName, streamSegmentId, getContainerId());
            this.metadataByName.put(streamSegmentName, segmentMetadata);
            this.metadataById.put(streamSegmentId, segmentMetadata);
        }

        segmentMetadata.setLastUsed(getOperationSequenceNumber());
        log.info("{}: MapStreamSegment Id = {}, Name = '{}'", this.traceObjectId, streamSegmentId, streamSegmentName);
        return segmentMetadata;
    }

    @Override
    public UpdateableSegmentMetadata mapStreamSegmentId(String streamSegmentName, long streamSegmentId, long parentStreamSegmentId) {
        StreamSegmentMetadata segmentMetadata;
        synchronized (this.lock) {
            validateNewMapping(streamSegmentName, streamSegmentId);
            StreamSegmentMetadata parentMetadata = this.metadataById.getOrDefault(parentStreamSegmentId, null);
            Exceptions.checkArgument(parentMetadata != null, "parentStreamSegmentId", "Invalid Parent Stream Id.");
            Exceptions.checkArgument(parentMetadata.getParentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID,
                    "parentStreamSegmentId", "Cannot create a transaction StreamSegment for another transaction StreamSegment.");

            segmentMetadata = new StreamSegmentMetadata(streamSegmentName, streamSegmentId, parentStreamSegmentId, getContainerId());
            this.metadataByName.put(streamSegmentName, segmentMetadata);
            this.metadataById.put(streamSegmentId, segmentMetadata);
        }

        segmentMetadata.setLastUsed(getOperationSequenceNumber());
        log.info("{}: MapTransactionStreamSegment ParentId = {}, Id = {}, Name = '{}'", this.traceObjectId, parentStreamSegmentId, streamSegmentId, streamSegmentName);
        return segmentMetadata;
    }

    @GuardedBy("lock")
    private void validateNewMapping(String streamSegmentName, long streamSegmentId) {
        Exceptions.checkArgument(!this.metadataByName.containsKey(streamSegmentName), "streamSegmentName",
                "StreamSegment '%s' is already mapped.", streamSegmentName);
        Exceptions.checkArgument(!this.metadataById.containsKey(streamSegmentId), "streamSegmentId",
                "StreamSegment Id %d is already mapped.", streamSegmentId);
        if (!this.recoveryMode.get()) {
            // We enforce the max active segment count only in non-recovery mode. If for some reason we manage to recover
            // more than this number of segments, then we shouldn't block recovery for that (it likely means we have a problem
            // somewhere else though).
            Preconditions.checkState(this.metadataById.size() < this.maxActiveSegmentCount,
                    "StreamSegment '%s' cannot be mapped because the maximum allowed number of mapped segments (%s)has been reached.",
                    streamSegmentName, this.maxActiveSegmentCount);
        }
    }

    @Override
    public Collection<Long> getAllStreamSegmentIds() {
        return this.metadataById.keySet();
    }

    @Override
    public Collection<SegmentMetadata> deleteStreamSegment(String streamSegmentName) {
        Collection<SegmentMetadata> result = new ArrayList<>();
        synchronized (this.lock) {
            StreamSegmentMetadata segmentMetadata = this.metadataByName.getOrDefault(streamSegmentName, null);
            if (segmentMetadata == null) {
                // We have no knowledge in our metadata about this StreamSegment. This means it has no transactions associated
                // with it, so no need to do anything else.
                log.info("{}: DeleteStreamSegments (nothing)", this.traceObjectId);
                return result;
            }

            // Mark this segment as deleted.
            result.add(segmentMetadata);
            segmentMetadata.markDeleted();

            // Find any transactions that point to this StreamSegment (as a parent).
            this.metadataById
                    .values().stream()
                    .filter(m -> m.getParentId() == segmentMetadata.getId())
                    .forEach(m -> {
                        m.markDeleted();
                        result.add(m);
                    });
        }

        log.info("{}: DeleteStreamSegments {}", this.traceObjectId, result);
        return result;
    }

    @Override
    public long nextOperationSequenceNumber() {
        ensureNonRecoveryMode();
        return this.sequenceNumber.incrementAndGet();
    }

    @Override
    public void setOperationSequenceNumber(long value) {
        ensureRecoveryMode();

        // Note: This check-and-set is not atomic, but in recovery mode we are executing in a single thread, so this is ok.
        Exceptions.checkArgument(value >= this.sequenceNumber.get(), "value", "Invalid SequenceNumber. Expecting greater than %d.", this.sequenceNumber.get());
        this.sequenceNumber.set(value);
    }

    @Override
    public void setContainerEpoch(long value) {
        ensureRecoveryMode();
        Preconditions.checkArgument(value > 0, "epoch must be a non-negative number");

        // Check and update epoch atomically.
        Preconditions.checkState(this.epoch.compareAndSet(Long.MIN_VALUE, value), "epoch has already been set.");
    }

    //endregion

    //region EvictableMetadata Implementation

    @Override
    public Collection<SegmentMetadata> getEvictionCandidates(long sequenceNumberCutoff, int maxCount) {
        long adjustedCutoff = Math.min(sequenceNumberCutoff, this.lastTruncatedSequenceNumber.get());
        List<SegmentMetadata> candidates;
        synchronized (this.lock) {
            // Find those segments that have active transactions associated with them.
            Set<Long> activeTransactions = getSegmentsWithActiveTransactions(adjustedCutoff);

            // The result is all segments that are eligible for removal but that do not have any active transactions.
            candidates = this.metadataById
                    .values().stream()
                    .filter(m -> isEligibleForEviction(m, adjustedCutoff) && !activeTransactions.contains(m.getId()))
                    .collect(Collectors.toList());
        }

        // If we have more candidates than were requested to return, then return only the ones that were not recently used.
        if (candidates.size() > maxCount) {
            candidates.sort(Comparator.comparingLong(SegmentMetadata::getLastUsed));
            candidates = candidates.subList(0, maxCount);
        }

        return candidates;
    }

    @Override
    public Collection<SegmentMetadata> cleanup(Collection<SegmentMetadata> evictionCandidates, long sequenceNumberCutoff) {
        long adjustedCutoff = Math.min(sequenceNumberCutoff, this.lastTruncatedSequenceNumber.get());
        Collection<SegmentMetadata> evictedSegments = new ArrayList<>(evictionCandidates.size());
        synchronized (this.lock) {
            // Find those segments that have active transactions associated with them.
            Set<Long> activeTransactions = getSegmentsWithActiveTransactions(adjustedCutoff);

            // Remove those candidates that are still eligible for removal and which do not have any active transactions.
            evictionCandidates
                    .stream()
                    .filter(m -> isEligibleForEviction(m, adjustedCutoff) && !activeTransactions.contains(m.getId()))
                    .forEach(m -> {
                        this.metadataById.remove(m.getId());
                        this.metadataByName.remove(m.getName());
                        evictedSegments.add(m);
                    });
        }

        log.info("{}: EvictedStreamSegments {}", this.traceObjectId, evictedSegments);
        return evictedSegments;
    }

    /**
     * Gets a Set of Segment Ids that have active transactions referring to them.
     *
     * @param sequenceNumberCutoff A Sequence Number that indicates the cutoff threshold. A Segment is eligible for eviction
     *                             if it has a LastUsed value smaller than this threshold.
     * @return A Set of Segment Ids that have active transactions referring to them.
     */
    @GuardedBy("lock")
    private Set<Long> getSegmentsWithActiveTransactions(long sequenceNumberCutoff) {
        return this.metadataById
                .values().stream()
                .filter(m -> m.getParentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID
                        && !isEligibleForEviction(m, sequenceNumberCutoff))
                .map(SegmentMetadata::getParentId)
                .collect(Collectors.toSet());
    }

    /**
     * Determines whether the Segment with given metadata can be evicted, based on the the given Sequence Number Threshold.
     *
     * @param metadata             The Metadata for the Segment that is considered for eviction.
     * @param sequenceNumberCutoff A Sequence Number that indicates the cutoff threshold. A Segment is eligible for eviction
     *                             if it has a LastUsed value smaller than this threshold. One exception to this rule
     *                             is deleted segments, which only need to be truncated out of the Log.
     * @return True if the Segment can be evicted, false otherwise.
     */
    private boolean isEligibleForEviction(SegmentMetadata metadata, long sequenceNumberCutoff) {
        return metadata.getLastUsed() < sequenceNumberCutoff
                || metadata.isDeleted() && metadata.getLastUsed() <= this.lastTruncatedSequenceNumber.get();
    }

    //endregion

    //region RecoverableMetadata Implementation

    @Override
    public void enterRecoveryMode() {
        ensureNonRecoveryMode();
        this.recoveryMode.set(true);
        log.info("{}: Enter RecoveryMode.", this.traceObjectId);
    }

    @Override
    public void exitRecoveryMode() {
        ensureRecoveryMode();
        this.recoveryMode.set(false);
        log.info("{}: Exit RecoveryMode.", this.traceObjectId);
    }

    @Override
    public void reset() {
        ensureRecoveryMode();
        this.sequenceNumber.set(0);
        this.lastTruncatedSequenceNumber.set(0);
        this.epoch.set(NO_EPOCH);
        synchronized (this.lock) {
            this.metadataByName.clear();
            this.metadataById.clear();
        }

        synchronized (this.truncationMarkers) {
            this.truncationMarkers.clear();
            this.truncationPoints.clear();
        }

        log.info("{}: Reset.", this.traceObjectId);
    }

    private void ensureRecoveryMode() {
        Preconditions.checkState(isRecoveryMode(), "StreamSegmentContainerMetadata is not in recovery mode. Cannot execute this operation.");
    }

    private void ensureNonRecoveryMode() {
        Preconditions.checkState(!isRecoveryMode(), "StreamSegmentContainerMetadata is in recovery mode. Cannot execute this operation.");
    }

    //endregion

    //region TruncationMarkerRepository Implementation

    @Override
    public void recordTruncationMarker(long operationSequenceNumber, LogAddress address) {
        Exceptions.checkArgument(operationSequenceNumber >= 0, "operationSequenceNumber", "Operation Sequence Number must be a positive number.");
        Preconditions.checkNotNull(address, "address");
        synchronized (this.truncationMarkers) {
            this.truncationMarkers.put(operationSequenceNumber, address);
        }
    }

    @Override
    public void removeTruncationMarkers(long upToOperationSequenceNumber) {
        synchronized (this.truncationMarkers) {
            this.truncationMarkers.headMap(upToOperationSequenceNumber, true).clear();
            this.truncationPoints.headSet(upToOperationSequenceNumber, true).clear();
        }

        this.lastTruncatedSequenceNumber.set(upToOperationSequenceNumber);
    }

    @Override
    public LogAddress getClosestTruncationMarker(long operationSequenceNumber) {
        Map.Entry<Long, LogAddress> result;
        synchronized (this.truncationMarkers) {
            result = this.truncationMarkers.floorEntry(operationSequenceNumber);
        }

        return result == null ? null : result.getValue();
    }

    @Override
    public void setValidTruncationPoint(long sequenceNumber) {
        Exceptions.checkArgument(sequenceNumber >= 0, "sequenceNumber", "Operation Sequence Number must be a positive number.");
        synchronized (this.truncationMarkers) {
            this.truncationPoints.add(sequenceNumber);
        }
    }

    @Override
    public boolean isValidTruncationPoint(long sequenceNumber) {
        synchronized (this.truncationMarkers) {
            return this.truncationPoints.contains(sequenceNumber);
        }
    }

    @Override
    public long getClosestValidTruncationPoint(long sequenceNumber) {
        Long result;
        synchronized (this.truncationMarkers) {
            result = this.truncationPoints.floor(sequenceNumber);
        }

        return result == null ? Operation.NO_SEQUENCE_NUMBER : result;
    }

    //endregion
}
