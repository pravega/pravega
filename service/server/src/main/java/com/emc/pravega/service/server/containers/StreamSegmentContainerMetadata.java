/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.storage.LogAddress;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
public class StreamSegmentContainerMetadata implements UpdateableContainerMetadata {
    //region Members

    private final String traceObjectId;
    private final AtomicLong sequenceNumber;
    private final AtomicLong lastTruncatedSequenceNumber;
    @GuardedBy("lock")
    private final HashMap<String, StreamSegmentMetadata> metadataByName;
    @GuardedBy("lock")
    private final HashMap<Long, StreamSegmentMetadata> metadataById;
    private final AtomicBoolean recoveryMode;
    private final int streamSegmentContainerId;
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
     */
    public StreamSegmentContainerMetadata(int streamSegmentContainerId) {
        this.traceObjectId = String.format("SegmentContainer[%d]", streamSegmentContainerId);
        this.streamSegmentContainerId = streamSegmentContainerId;
        this.sequenceNumber = new AtomicLong();
        this.metadataByName = new HashMap<>();
        this.metadataById = new HashMap<>();
        this.truncationMarkers = new TreeMap<>();
        this.truncationPoints = new TreeSet<>();
        this.recoveryMode = new AtomicBoolean();
        this.lastTruncatedSequenceNumber = new AtomicLong();
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
    public UpdateableSegmentMetadata mapStreamSegmentId(String streamSegmentName, long streamSegmentId) {
        StreamSegmentMetadata segmentMetadata;
        synchronized (this.lock) {
            Exceptions.checkArgument(!this.metadataByName.containsKey(streamSegmentName), "streamSegmentName", "StreamSegment '%s' is already mapped.", streamSegmentName);
            Exceptions.checkArgument(!this.metadataById.containsKey(streamSegmentId), "streamSegmentId", "StreamSegment Id %d is already mapped.", streamSegmentId);

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
            Exceptions.checkArgument(!this.metadataByName.containsKey(streamSegmentName), "streamSegmentName", "StreamSegment '%s' is already mapped.", streamSegmentName);
            Exceptions.checkArgument(!this.metadataById.containsKey(streamSegmentId), "streamSegmentId", "StreamSegment Id %d is already mapped.", streamSegmentId);

            StreamSegmentMetadata parentMetadata = this.metadataById.getOrDefault(parentStreamSegmentId, null);
            Exceptions.checkArgument(parentMetadata != null, "parentStreamSegmentId", "Invalid Parent Stream Id.");
            Exceptions.checkArgument(parentMetadata.getParentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID, "parentStreamSegmentId", "Cannot create a transaction StreamSegment for another transaction StreamSegment.");

            segmentMetadata = new StreamSegmentMetadata(streamSegmentName, streamSegmentId, parentStreamSegmentId, getContainerId());
            this.metadataByName.put(streamSegmentName, segmentMetadata);
            this.metadataById.put(streamSegmentId, segmentMetadata);
        }

        segmentMetadata.setLastUsed(getOperationSequenceNumber());
        log.info("{}: MapTransactionStreamSegment ParentId = {}, Id = {}, Name = '{}'", this.traceObjectId, parentStreamSegmentId, streamSegmentId, streamSegmentName);
        return segmentMetadata;
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
    public Collection<SegmentMetadata> getEvictionCandidates(long sequenceNumberCutoff) {
        long adjustedCutoff = Math.min(sequenceNumberCutoff, this.lastTruncatedSequenceNumber.get());
        synchronized (this.lock) {
            // Find those segments that have active transactions associated with them.
            Set<Long> activeTransactions = getSegmentsWithActiveTransactions(adjustedCutoff);

            // The result is all segments that are eligible for removal but that do not have any active transactions.
            return this.metadataById
                    .values().stream()
                    .filter(m -> isEligibleForEviction(m, adjustedCutoff) && !activeTransactions.contains(m.getId()))
                    .collect(Collectors.toList());
        }
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
     *                             if it has a LastUsed value smaller than this threshold.
     * @return True if the Segment can be evicted, false otherwise.
     */
    private boolean isEligibleForEviction(SegmentMetadata metadata, long sequenceNumberCutoff) {
        return metadata.getLastUsed() < sequenceNumberCutoff;
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
