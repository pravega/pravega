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

package com.emc.logservice.server.reading;

import com.emc.logservice.common.AutoReleaseLock;
import com.emc.logservice.common.Exceptions;
import com.emc.logservice.common.ReadWriteAutoReleaseLock;
import com.emc.logservice.contracts.ReadResult;
import com.emc.logservice.server.Cache;
import com.emc.logservice.server.SegmentMetadata;
import com.emc.logservice.server.SegmentMetadataCollection;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * StreamSegment Container Read Index. Provides access to Read Indices for all StreamSegments within this Container.
 * <p>
 * Integrates reading data from the following sources:
 * <ol>
 * <li> The tail-end part of the StreamSegment (the part that is in DurableLog, but not yet in Storage).
 * <li> The part of the StreamSegment that is in Storage, but not in DurableLog. This data will be brought into memory
 * for fast read-ahead access.
 * </ol>
 */
@Slf4j
public class ReadIndex implements Cache {
    //region Members

    private final String traceObjectId;
    private final String containerId;
    private final HashMap<Long, StreamSegmentReadIndex> readIndices;
    private final ReadWriteAutoReleaseLock lock = new ReadWriteAutoReleaseLock();
    private SegmentMetadataCollection metadata;
    private boolean recoveryMode;
    private boolean closed;

    //endregion

    //region Constructor

    public ReadIndex(SegmentMetadataCollection metadata, String containerId) {
        Preconditions.checkNotNull(metadata, "metadata");
        this.traceObjectId = String.format("ReadIndex[%s]", containerId);
        this.containerId = containerId;
        this.readIndices = new HashMap<>();
        this.metadata = metadata;
        this.recoveryMode = false;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;
            try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
                // Need to close all individual read indices in order to cancel Readers and Future Reads.
                this.readIndices.values().forEach(StreamSegmentReadIndex::close);
                this.readIndices.clear();
            }

            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region Cache Implementation

    @Override
    public void append(long streamSegmentId, long offset, byte[] data) {
        Exceptions.checkNotClosed(this.closed, this);
        log.debug("{}: append (StreamSegmentId = {}, Offset = {}, DataLength = {}).", this.traceObjectId, streamSegmentId, offset, data.length);

        // Append the data to the StreamSegment Index. It performs further validation with respect to offsets, etc.
        StreamSegmentReadIndex index = getReadIndex(streamSegmentId, true);
        Exceptions.checkArgument(!index.isMerged(), "streamSegmentId", "StreamSegment is merged. Cannot append to it anymore.");
        index.append(offset, data);
    }

    @Override
    public void beginMerge(long targetStreamSegmentId, long offset, long sourceStreamSegmentId, long sourceStreamSegmentLength) {
        Exceptions.checkNotClosed(this.closed, this);
        log.debug("{}: beginMerge (TargetId = {}, Offset = {}, SourceId = {}, SourceLength = {}).", this.traceObjectId, targetStreamSegmentId, offset, sourceStreamSegmentId, sourceStreamSegmentLength);

        StreamSegmentReadIndex targetIndex = getReadIndex(targetStreamSegmentId, true);
        StreamSegmentReadIndex sourceIndex = getReadIndex(sourceStreamSegmentId, true);
        Exceptions.checkArgument(!targetIndex.isMerged(), "targetStreamSegmentId", "StreamSegment is merged. Cannot access it anymore.");
        Exceptions.checkArgument(!sourceIndex.isMerged(), "sourceStreamSegmentId", "StreamSegment is merged. Cannot access it anymore.");
        targetIndex.beginMerge(offset, sourceIndex);
        sourceIndex.markMerged();
    }

    @Override
    public void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId) {
        Exceptions.checkNotClosed(this.closed, this);
        log.debug("{}: completeMerge (TargetId = {}, SourceId = {}.", this.traceObjectId, targetStreamSegmentId, sourceStreamSegmentId);

        StreamSegmentReadIndex targetIndex = getReadIndex(targetStreamSegmentId, true);
        targetIndex.completeMerge(sourceStreamSegmentId);
        removeReadIndex(sourceStreamSegmentId);
    }

    @Override
    public ReadResult read(long streamSegmentId, long offset, int maxLength, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        log.debug("{}: read (StreamSegmentId = {}, Offset = {}, MaxLength = {}).", this.traceObjectId, streamSegmentId, offset, maxLength);

        StreamSegmentReadIndex index = getReadIndex(streamSegmentId, true);
        Exceptions.checkArgument(!index.isMerged(), "streamSegmentId", "StreamSegment is merged. Cannot access it anymore.");
        return index.read(offset, maxLength, timeout);
    }

    @Override
    public void triggerFutureReads(Collection<Long> streamSegmentIds) {
        Exceptions.checkNotClosed(this.closed, this);
        log.debug("{}: triggerFutureReads (StreamSegmentIds = {}).", this.traceObjectId, streamSegmentIds);

        HashSet<String> missingIds = new HashSet<>();
        for (long ssId : streamSegmentIds) {
            StreamSegmentReadIndex index = getReadIndex(ssId, false);
            if (index == null) {
                if (this.metadata.getStreamSegmentMetadata(ssId) == null) {
                    missingIds.add(Long.toString(ssId));
                }
            } else {
                index.triggerFutureReads();
            }
        }

        // Throw any exception at the end - we want to make sure at least the ones that did have a valid index entry got triggered.
        Exceptions.checkArgument(missingIds.size() == 0, "streamSegmentIds", "At least one StreamSegmentId does not exist in the metadata: {}", missingIds);
    }

    @Override
    public void clear() {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.recoveryMode, "Read Index is not in recovery mode. Cannot clear cache.");
        log.info("{}: Cleared.", this.traceObjectId);

        try (AutoReleaseLock ignored = lock.acquireWriteLock()) {
            this.readIndices.values().forEach(StreamSegmentReadIndex::close);
            this.readIndices.clear();
        }
    }

    @Override
    public void performGarbageCollection() {
        Exceptions.checkNotClosed(this.closed, this);

        List<Long> toRemove = new ArrayList<>();
        try (AutoReleaseLock ignored = lock.acquireWriteLock()) {
            for (Long streamSegmentId : this.readIndices.keySet()) {
                SegmentMetadata segmentMetadata = this.metadata.getStreamSegmentMetadata(streamSegmentId);
                if (segmentMetadata == null || segmentMetadata.isDeleted()) {
                    toRemove.add(streamSegmentId);
                }
            }

            toRemove.forEach(streamSegmentId -> this.readIndices.remove(streamSegmentId).close());
        }

        log.info("{}: Garbage Collection Complete (Removed {}).", this.traceObjectId, toRemove);
    }

    @Override
    public void enterRecoveryMode(SegmentMetadataCollection recoveryMetadataSource) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!this.recoveryMode, "Read Index is already in recovery mode.");
        Preconditions.checkNotNull(recoveryMetadataSource, "recoveryMetadataSource");

        this.recoveryMode = true;
        this.metadata = recoveryMetadataSource;
        log.info("{} Enter RecoveryMode.", this.traceObjectId);
        clear();
    }

    @Override
    public void exitRecoveryMode(SegmentMetadataCollection finalMetadataSource, boolean success) {
        Exceptions.checkNotClosed(this.closed, this);

        Preconditions.checkState(this.recoveryMode, "Read Index is not in recovery mode.");
        Preconditions.checkNotNull(finalMetadataSource, "finalMetadataSource");

        if (success) {
            for (Map.Entry<Long, StreamSegmentReadIndex> e : this.readIndices.entrySet()) {
                SegmentMetadata metadata = finalMetadataSource.getStreamSegmentMetadata(e.getKey());
                Exceptions.checkArgument(metadata != null, "finalMetadataSource", "Final Metadata has no knowledge of StreamSegment Id {}.", e.getKey());
                e.getValue().exitRecoveryMode(metadata);
            }
        } else {
            // Recovery was unsuccessful. Clear the contents of the cache to avoid further issues.
            clear();
        }

        this.metadata = finalMetadataSource;
        this.recoveryMode = false;
        log.info("{} Exit RecoveryMode.", this.traceObjectId);
    }

    //endregion

    //region Helpers

    /**
     * Gets a reference to the existing StreamSegmentRead index for the given StreamSegment Id.
     *
     * @param streamSegmentId
     * @param createIfNotPresent If no Read Index is loaded, creates a new one.
     * @return
     */
    private StreamSegmentReadIndex getReadIndex(long streamSegmentId, boolean createIfNotPresent) {
        StreamSegmentReadIndex index;
        try (AutoReleaseLock readLock = lock.acquireReadLock()) {
            // Try to see if we have the index already in memory.
            index = this.readIndices.getOrDefault(streamSegmentId, null);
            if (index != null || !createIfNotPresent) {
                // If we do, or we are told not to create one if not present, then return whatever we have.
                return index;
            }

            try (AutoReleaseLock writeLock = lock.upgradeToWriteLock(readLock)) {
                // We don't have it; we have acquired the exclusive write lock, and check again, just in case, if someone
                // else got it for us.
                index = this.readIndices.getOrDefault(streamSegmentId, null);
                if (index != null) {
                    return index;
                }

                // We don't have it, and nobody else got it for us.
                SegmentMetadata ssm = this.metadata.getStreamSegmentMetadata(streamSegmentId);
                Exceptions.checkArgument(ssm != null, "streamSegmentId", "StreamSegmentId {} does not exist in the metadata.", streamSegmentId);
                Exceptions.checkArgument(!ssm.isDeleted(), "streamSegmentId", "StreamSegmentId {} exists in the metadata but is marked as deleted.", streamSegmentId);

                index = new StreamSegmentReadIndex(ssm, this.recoveryMode, this.containerId);
                this.readIndices.put(streamSegmentId, index);
            }
        }

        return index;
    }

    private boolean removeReadIndex(long streamSegmentId) {
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            StreamSegmentReadIndex index = this.readIndices.remove(streamSegmentId);
            if (index != null) {
                index.close();
            }

            return index != null;
        }
    }

    //endregion
}
