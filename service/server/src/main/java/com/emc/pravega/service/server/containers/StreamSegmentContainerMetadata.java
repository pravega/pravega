/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.util.CollectionHelpers;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metadata for a Stream Segment Container.
 */
@Slf4j
public class StreamSegmentContainerMetadata implements UpdateableContainerMetadata {
    //region Members

    private final String traceObjectId;
    private final AtomicLong sequenceNumber;
    @GuardedBy("lock")
    private final AbstractMap<String, Long> streamSegmentIds;
    @GuardedBy("lock")
    private final AbstractMap<Long, UpdateableSegmentMetadata> segmentMetadata;
    private final AtomicBoolean recoveryMode;
    private final int streamSegmentContainerId;
    @GuardedBy("lock")
    private final TreeMap<Long, LogAddress> truncationMarkers;
    @GuardedBy("lock")
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
        this.streamSegmentIds = new HashMap<>();
        this.segmentMetadata = new HashMap<>();
        this.truncationMarkers = new TreeMap<>();
        this.truncationPoints = new TreeSet<>();
        this.recoveryMode = new AtomicBoolean();
    }

    //endregion

    //region SegmentMetadataCollection Implementation

    /**
     * Gets the Id of the StreamSegment with given name.
     *
     * @param streamSegmentName The case-sensitive StreamSegment name.
     * @return The Id of the StreamSegment, or NO_STREAM_SEGMENT_ID if the Metadata has no knowledge of it.
     */
    public long getStreamSegmentId(String streamSegmentName) {
        synchronized (this.lock) {
            return this.streamSegmentIds.getOrDefault(streamSegmentName, NO_STREAM_SEGMENT_ID);
        }
    }

    @Override
    public UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) {
        synchronized (this.lock) {
            return this.segmentMetadata.getOrDefault(streamSegmentId, null);
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
        UpdateableSegmentMetadata segmentMetadata;
        synchronized (this.lock) {
            Exceptions.checkArgument(!this.streamSegmentIds.containsKey(streamSegmentName), "streamSegmentName", "StreamSegment '%s' is already mapped.", streamSegmentName);
            Exceptions.checkArgument(!this.segmentMetadata.containsKey(streamSegmentId), "streamSegmentId", "StreamSegment Id %d is already mapped.", streamSegmentId);

            segmentMetadata = new StreamSegmentMetadata(streamSegmentName, streamSegmentId, getContainerId());
            this.streamSegmentIds.put(streamSegmentName, streamSegmentId);
            this.segmentMetadata.put(streamSegmentId, segmentMetadata);
        }

        log.info("{}: MapStreamSegment Id = {}, Name = '{}'", this.traceObjectId, streamSegmentId, streamSegmentName);
        return segmentMetadata;
    }

    @Override
    public UpdateableSegmentMetadata mapStreamSegmentId(String streamSegmentName, long streamSegmentId, long parentStreamSegmentId) {
        UpdateableSegmentMetadata segmentMetadata;
        synchronized (this.lock) {
            Exceptions.checkArgument(!this.streamSegmentIds.containsKey(streamSegmentName), "streamSegmentName", "StreamSegment '%s' is already mapped.", streamSegmentName);
            Exceptions.checkArgument(!this.segmentMetadata.containsKey(streamSegmentId), "streamSegmentId", "StreamSegment Id %d is already mapped.", streamSegmentId);

            UpdateableSegmentMetadata parentMetadata = this.segmentMetadata.getOrDefault(parentStreamSegmentId, null);
            Exceptions.checkArgument(parentMetadata != null, "parentStreamSegmentId", "Invalid Parent Stream Id.");
            Exceptions.checkArgument(parentMetadata.getParentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID, "parentStreamSegmentId", "Cannot create a transaction StreamSegment for another transaction StreamSegment.");

            segmentMetadata = new StreamSegmentMetadata(streamSegmentName, streamSegmentId, parentStreamSegmentId, getContainerId());
            this.streamSegmentIds.put(streamSegmentName, streamSegmentId);
            this.segmentMetadata.put(streamSegmentId, segmentMetadata);
        }

        log.info("{}: MapTransactionStreamSegment ParentId = {}, Id = {}, Name = '{}'", this.traceObjectId, parentStreamSegmentId, streamSegmentId, streamSegmentName);
        return segmentMetadata;
    }

    @Override
    public Collection<Long> getAllStreamSegmentIds() {
        return this.segmentMetadata.keySet();
    }

    @Override
    public Map<Long, String> deleteStreamSegment(String streamSegmentName) {
        Map<Long, String> result = new HashMap<>();
        synchronized (this.lock) {
            long streamSegmentId = this.streamSegmentIds.getOrDefault(streamSegmentName, ContainerMetadata.NO_STREAM_SEGMENT_ID);
            result.put(streamSegmentId, streamSegmentName);
            if (streamSegmentId == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                // We have no knowledge in our metadata about this StreamSegment. This means it has no transactions associated
                // with it, so no need to do anything else.
                log.info("{}: DeleteStreamSegments {}", this.traceObjectId, result);
                return result;
            }

            // Mark this segment as deleted.
            UpdateableSegmentMetadata segmentMetadata = this.segmentMetadata.getOrDefault(streamSegmentId, null);
            if (segmentMetadata != null) {
                segmentMetadata.markDeleted();
            }

            // Find any transactions that point to this StreamSegment (as a parent).
            CollectionHelpers.forEach(
                    this.segmentMetadata.values(),
                    m -> m.getParentId() == streamSegmentId,
                    m -> {
                        m.markDeleted();
                        result.put(m.getId(), m.getName());
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
        synchronized (this.lock) {
            this.streamSegmentIds.clear();
            this.segmentMetadata.clear();
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
        ArrayList<Long> toRemove = new ArrayList<>();
        synchronized (this.truncationMarkers) {
            // Remove Truncation Markers.
            toRemove.addAll(this.truncationMarkers.headMap(upToOperationSequenceNumber, true).keySet());
            toRemove.forEach(this.truncationMarkers::remove);

            // Remove Truncation points
            toRemove.clear();
            toRemove.addAll(this.truncationPoints.headSet(upToOperationSequenceNumber, true));
            this.truncationPoints.removeAll(toRemove);
        }
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
