/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.logservice.server.containers;

import com.emc.logservice.common.AutoReleaseLock;
import com.emc.logservice.common.Exceptions;
import com.emc.logservice.common.ReadWriteAutoReleaseLock;
import com.emc.logservice.server.SegmentMetadataCollection;
import com.emc.logservice.server.UpdateableContainerMetadata;
import com.emc.logservice.server.UpdateableSegmentMetadata;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private final AbstractMap<String, Long> streamSegmentIds;
    private final AbstractMap<Long, UpdateableSegmentMetadata> segmentMetadata;
    private final AtomicBoolean recoveryMode;
    private final String streamSegmentContainerId;
    private final AbstractMap<Long, Long> truncationMarkers;
    private final HashSet<Long> truncationPoints;
    private final ReadWriteAutoReleaseLock lock = new ReadWriteAutoReleaseLock();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentContainerMetadata.
     */
    public StreamSegmentContainerMetadata(String streamSegmentContainerId) {
        Exceptions.checkNotNullOrEmpty(streamSegmentContainerId, "streamSegmentContainerId");
        this.traceObjectId = String.format("SegmentContainer[%s]", streamSegmentContainerId);
        this.streamSegmentContainerId = streamSegmentContainerId;
        this.sequenceNumber = new AtomicLong();
        this.streamSegmentIds = new HashMap<>();
        this.segmentMetadata = new HashMap<>();
        this.truncationMarkers = new ConcurrentHashMap<>();
        this.truncationPoints = new HashSet<>();
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
        try (AutoReleaseLock ignored = this.lock.acquireReadLock()) {
            return this.streamSegmentIds.getOrDefault(streamSegmentName, NO_STREAM_SEGMENT_ID);
        }
    }

    @Override
    public UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) {
        try (AutoReleaseLock ignored = this.lock.acquireReadLock()) {
            return this.segmentMetadata.getOrDefault(streamSegmentId, null);
        }
    }

    //endregion

    //region ContainerMetadata Implementation

    @Override
    public String getContainerId() {
        return this.streamSegmentContainerId;
    }

    @Override
    public boolean isRecoveryMode() {
        return this.recoveryMode.get();
    }

    @Override
    public long getNewOperationSequenceNumber() {
        ensureNonRecoveryMode();
        return this.sequenceNumber.incrementAndGet();
    }

    //endregion

    //region UpdateableContainerMetadata

    @Override
    public void mapStreamSegmentId(String streamSegmentName, long streamSegmentId) {
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            Exceptions.checkArgument(!this.streamSegmentIds.containsKey(streamSegmentName), "streamSegmentName", "StreamSegment '%s' is already mapped.", streamSegmentName);
            Exceptions.checkArgument(!this.segmentMetadata.containsKey(streamSegmentId), "streamSegmentId", "StreamSegment Id %d is already mapped.", streamSegmentId);

            this.streamSegmentIds.put(streamSegmentName, streamSegmentId);
            this.segmentMetadata.put(streamSegmentId, new StreamSegmentMetadata(streamSegmentName, streamSegmentId));
        }

        log.info("{}: MapStreamSegment Id = {}, Name = '{}'", this.traceObjectId, streamSegmentId, streamSegmentName);
    }

    @Override
    public void mapStreamSegmentId(String streamSegmentName, long streamSegmentId, long parentStreamSegmentId) {
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            Exceptions.checkArgument(!this.streamSegmentIds.containsKey(streamSegmentName), "streamSegmentName", "StreamSegment '%s' is already mapped.", streamSegmentName);
            Exceptions.checkArgument(!this.segmentMetadata.containsKey(streamSegmentId), "streamSegmentId", "StreamSegment Id %d is already mapped.", streamSegmentId);

            UpdateableSegmentMetadata parentMetadata = this.segmentMetadata.getOrDefault(parentStreamSegmentId, null);
            Exceptions.checkArgument(parentMetadata != null, "parentStreamSegmentId", "Invalid Parent Stream Id.");
            Exceptions.checkArgument(parentMetadata.getParentId() == SegmentMetadataCollection.NO_STREAM_SEGMENT_ID, "parentStreamSegmentId", "Cannot create a batch StreamSegment for another batch StreamSegment.");

            this.streamSegmentIds.put(streamSegmentName, streamSegmentId);
            this.segmentMetadata.put(streamSegmentId, new StreamSegmentMetadata(streamSegmentName, streamSegmentId, parentStreamSegmentId));
        }

        log.info("{}: MapBatchStreamSegment ParentId = {}, Id = {}, Name = '{}'", this.traceObjectId, parentStreamSegmentId, streamSegmentId, streamSegmentName);
    }

    @Override
    public Collection<Long> getAllStreamSegmentIds() {
        return this.segmentMetadata.keySet();
    }

    @Override
    public Collection<String> deleteStreamSegment(String streamSegmentName) {
        Collection<String> result = new ArrayList<>();
        result.add(streamSegmentName);
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            long streamSegmentId = this.streamSegmentIds.getOrDefault(streamSegmentName, SegmentMetadataCollection.NO_STREAM_SEGMENT_ID);
            if (streamSegmentId == SegmentMetadataCollection.NO_STREAM_SEGMENT_ID) {
                // We have no knowledge in our metadata about this StreamSegment. This means it has no batches associated
                // with it, so no need to do anything else.
                log.info("{}: DeleteStreamSegments {}", this.traceObjectId, result);
                return result;
            }

            // Mark this segment as deleted.
            UpdateableSegmentMetadata segmentMetadata = this.segmentMetadata.getOrDefault(streamSegmentId, null);
            if (segmentMetadata != null) {
                segmentMetadata.markDeleted();
            }

            // Find any batches that point to this StreamSegment (as a parent).
            for (UpdateableSegmentMetadata batchSegmentMetadata : this.segmentMetadata.values()) {
                if (batchSegmentMetadata.getParentId() == streamSegmentId) {
                    batchSegmentMetadata.markDeleted();
                    result.add(batchSegmentMetadata.getName());
                }
            }
        }

        log.info("{}: DeleteStreamSegments {}", this.traceObjectId, result);
        return result;
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
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
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
    public void recordTruncationMarker(long operationSequenceNumber, long dataFrameSequenceNumber) {
        Exceptions.checkArgument(operationSequenceNumber >= 0, "operationSequenceNumber", "Operation Sequence Number must be a positive number.");
        Exceptions.checkArgument(dataFrameSequenceNumber >= 0, "dataFrameSequenceNumber", "DataFrame Sequence Number must be a positive number.");
        synchronized (this.truncationMarkers) {
            this.truncationMarkers.put(operationSequenceNumber, dataFrameSequenceNumber);
        }
    }

    @Override
    public void removeTruncationMarkers(long upToOperationSequenceNumber) {
        ArrayList<Long> toRemove = new ArrayList<>();
        synchronized (this.truncationMarkers) {
            // Remove Truncation Markers.
            this.truncationMarkers.keySet().forEach(key ->
            {
                if (key <= upToOperationSequenceNumber) {
                    toRemove.add(key);
                }
            });

            toRemove.forEach(this.truncationMarkers::remove);

            // Remove Truncation points
            toRemove.clear();
            this.truncationPoints.forEach(key ->
            {
                if (key <= upToOperationSequenceNumber) {
                    toRemove.add(key);
                }
            });
            toRemove.forEach(this.truncationPoints::remove);
        }
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
        return this.truncationPoints.contains(sequenceNumber);
    }

    @Override
    public long getClosestTruncationMarker(long operationSequenceNumber) {
        //TODO: make more efficient, maybe by using a different data structure, like TreeMap.
        Map.Entry<Long, Long> result = null;
        synchronized (this.truncationMarkers) {

            for (Map.Entry<Long, Long> tm : this.truncationMarkers.entrySet()) {
                long seqNo = tm.getKey();
                if (seqNo == operationSequenceNumber) {
                    // Found the best result.
                    return tm.getValue();
                } else if (seqNo < operationSequenceNumber) {
                    if (result == null || (result.getKey() < seqNo)) {
                        // We found a better result.
                        result = tm;
                    }
                }
            }
        }
        if (result == null) {
            return -1;
        } else {
            return result.getValue();
        }
    }

    //endregion
}
