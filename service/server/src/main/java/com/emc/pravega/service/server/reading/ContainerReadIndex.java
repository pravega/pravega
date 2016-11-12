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

package com.emc.pravega.service.server.reading;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.ReadOnlyStorage;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

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
public class ContainerReadIndex implements ReadIndex {
    //region Members

    private final String traceObjectId;
    @GuardedBy("lock")
    private final HashMap<Long, StreamSegmentReadIndex> readIndices;
    @GuardedBy("lock")
    private final Object lock = new Object();
    private final Cache cache;
    private final ReadOnlyStorage storage;
    private final Executor executor;
    private final ReadIndexConfig config;
    private final CacheManager cacheManager;
    private ContainerMetadata metadata;
    @GuardedBy("lock")
    private ContainerMetadata preRecoveryMetadata;
    @GuardedBy("lock")
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerReadIndex class.
     *
     * @param config       Configuration for the ReadIndex.
     * @param metadata     The ContainerMetadata to attach to.
     * @param cache        The cache to store data into.
     * @param storage      Storage to read data not in the ReadIndex from.
     * @param cacheManager The CacheManager to use for cache lifecycle management.
     * @param executor     An Executor to run async callbacks on.
     */
    public ContainerReadIndex(ReadIndexConfig config, ContainerMetadata metadata, Cache cache, ReadOnlyStorage
            storage, CacheManager cacheManager, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(cache, "cache");
        Preconditions.checkNotNull(storage, "storage");
        Preconditions.checkNotNull(cacheManager, "cacheManager");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkArgument(!metadata.isRecoveryMode(), "Given ContainerMetadata is in Recovery Mode.");

        this.traceObjectId = String.format("ReadIndex[%s]", metadata.getContainerId());
        this.readIndices = new HashMap<>();
        this.config = config;
        this.cache = cache;
        this.metadata = metadata;
        this.storage = storage;
        this.cacheManager = cacheManager;
        this.executor = executor;
        this.preRecoveryMetadata = null;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;
            synchronized (this.lock) {
                // Need to close all individual read indices in order to cancel Readers and Future Reads.
                this.readIndices.values().forEach(StreamSegmentReadIndex::close);
                this.readIndices.clear();
            }

            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region ReadIndex Implementation

    @Override
    public void append(long streamSegmentId, long offset, byte[] data) {
        Exceptions.checkNotClosed(this.closed, this);
        log.debug("{}: append (StreamSegmentId = {}, Offset = {}, DataLength = {}).", this.traceObjectId,
                streamSegmentId, offset, data.length);

        // Append the data to the StreamSegment Index. It performs further validation with respect to offsets, etc.
        StreamSegmentReadIndex index = getReadIndex(streamSegmentId, true);
        Exceptions.checkArgument(!index.isMerged(), "streamSegmentId", "StreamSegment is merged. Cannot append to it " +
                "anymore.");
        index.append(offset, data);
    }

    @Override
    public void beginMerge(long targetStreamSegmentId, long offset, long sourceStreamSegmentId) {
        Exceptions.checkNotClosed(this.closed, this);
        log.debug("{}: beginMerge (TargetId = {}, Offset = {}, SourceId = {}).", this.traceObjectId,
                targetStreamSegmentId, offset, sourceStreamSegmentId);

        StreamSegmentReadIndex targetIndex = getReadIndex(targetStreamSegmentId, true);
        StreamSegmentReadIndex sourceIndex = getReadIndex(sourceStreamSegmentId, true);
        Exceptions.checkArgument(!targetIndex.isMerged(), "targetStreamSegmentId", "StreamSegment is merged. Cannot " +
                "access it anymore.");
        Exceptions.checkArgument(!sourceIndex.isMerged(), "sourceStreamSegmentId", "StreamSegment is merged. Cannot " +
                "access it anymore.");
        targetIndex.beginMerge(offset, sourceIndex);
        sourceIndex.markMerged();
    }

    @Override
    public void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId) {
        Exceptions.checkNotClosed(this.closed, this);
        log.debug("{}: completeMerge (TargetId = {}, SourceId = {}.", this.traceObjectId, targetStreamSegmentId,
                sourceStreamSegmentId);

        StreamSegmentReadIndex targetIndex = getReadIndex(targetStreamSegmentId, true);
        targetIndex.completeMerge(sourceStreamSegmentId);
        removeReadIndex(sourceStreamSegmentId);
    }

    @Override
    public ReadResult read(long streamSegmentId, long offset, int maxLength, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        log.debug("{}: read (StreamSegmentId = {}, Offset = {}, MaxLength = {}).", this.traceObjectId,
                streamSegmentId, offset, maxLength);

        StreamSegmentReadIndex index = getReadIndex(streamSegmentId, true);
        Exceptions.checkArgument(!index.isMerged(), "streamSegmentId", "StreamSegment is merged. Cannot access it " +
                "anymore.");
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

        // Throw any exception at the end - we want to make sure at least the ones that did have a valid index entry
        // got triggered.
        Exceptions.checkArgument(missingIds.size() == 0, "streamSegmentIds", "At least one StreamSegmentId does not " +
                "exist in the metadata: %s", missingIds);
    }

    @Override
    public void clear() {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(isRecoveryMode(), "Read Index is not in recovery mode. Cannot clear ReadIndex.");
        log.info("{}: Cleared.", this.traceObjectId);

        synchronized (this.lock) {
            this.readIndices.values().forEach(StreamSegmentReadIndex::close);
            this.readIndices.clear();
        }
    }

    @Override
    public void performGarbageCollection() {
        Exceptions.checkNotClosed(this.closed, this);

        List<Long> toRemove = new ArrayList<>();
        synchronized (this.lock) {
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
    public void enterRecoveryMode(ContainerMetadata recoveryMetadataSource) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!isRecoveryMode(), "Read Index is already in recovery mode.");
        Preconditions.checkNotNull(recoveryMetadataSource, "recoveryMetadataSource");
        Preconditions.checkArgument(recoveryMetadataSource.isRecoveryMode(), "Given ContainerMetadata is not in " +
                "recovery mode.");
        Preconditions.checkArgument(this.metadata.getContainerId() == recoveryMetadataSource.getContainerId(), "Given" +
                " ContainerMetadata refers to a different container than this ReadIndex.");

        // Swap metadata with recovery metadata (but still keep track of recovery metadata.
        assert this.preRecoveryMetadata == null : "preRecoveryMetadata is not null, which should not happen unless we" +
                " already are in recovery mode";
        this.preRecoveryMetadata = this.metadata;
        this.metadata = recoveryMetadataSource;
        log.info("{} Enter RecoveryMode.", this.traceObjectId);
        clear();
    }

    @Override
    public void exitRecoveryMode(boolean successfulRecovery) throws DataCorruptionException {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.isRecoveryMode(), "Read Index is not in recovery mode.");
        assert this.preRecoveryMetadata != null : "preRecoveryMetadata not null, which should only be the case when " +
                "we are not in recovery mode";
        Preconditions.checkState(!this.preRecoveryMetadata.isRecoveryMode(), "Cannot take ReadIndex out of recovery: " +
                "ContainerMetadata is still in recovery mode.");

        if (successfulRecovery) {
            // Validate that the metadata has been properly recovered and that we are still in sync with it.
            for (Map.Entry<Long, StreamSegmentReadIndex> e : this.readIndices.entrySet()) {
                SegmentMetadata metadata = this.preRecoveryMetadata.getStreamSegmentMetadata(e.getKey());
                if (metadata == null) {
                    throw new DataCorruptionException(String.format("ContainerMetadata has no knowledge of " +
                            "StreamSegment Id %s.", e.getKey()));
                }

                e.getValue().exitRecoveryMode(metadata);
            }
        } else {
            // Recovery was unsuccessful. Clear the contents of the ReadIndex to avoid further issues.
            clear();
        }

        this.metadata = this.preRecoveryMetadata;
        this.preRecoveryMetadata = null;
        log.info("{} Exit RecoveryMode.", this.traceObjectId);
    }

    //endregion

    //region Helpers

    private boolean isRecoveryMode() {
        return this.preRecoveryMetadata != null;
    }

    /**
     * Gets a reference to the existing StreamSegmentRead index for the given StreamSegment Id.
     *
     * @param streamSegmentId    The Id of the StreamSegment whose ReadIndex to get.
     * @param createIfNotPresent If no Read Index is loaded, creates a new one.
     */
    private StreamSegmentReadIndex getReadIndex(long streamSegmentId, boolean createIfNotPresent) {
        StreamSegmentReadIndex index;
        synchronized (this.lock) {
            // Try to see if we have the index already in memory.
            index = this.readIndices.getOrDefault(streamSegmentId, null);
            if (index != null || !createIfNotPresent) {
                // If we do, or we are told not to create one if not present, then return whatever we have.
                return index;
            }

            // We don't have it, create one.
            SegmentMetadata segmentMetadata = this.metadata.getStreamSegmentMetadata(streamSegmentId);
            Exceptions.checkArgument(segmentMetadata != null, "streamSegmentId", "StreamSegmentId {} does not exist " +
                    "in the metadata.", streamSegmentId);
            Exceptions.checkArgument(!segmentMetadata.isDeleted(), "streamSegmentId", "StreamSegmentId {} exists in " +
                    "the metadata but is marked as deleted.", streamSegmentId);

            index = new StreamSegmentReadIndex(this.config, segmentMetadata, this.cache, this.storage, this.executor,
                    isRecoveryMode());
            this.cacheManager.register(index);
            this.readIndices.put(streamSegmentId, index);
        }

        return index;
    }

    private boolean removeReadIndex(long streamSegmentId) {
        synchronized (this.lock) {
            StreamSegmentReadIndex index = this.readIndices.remove(streamSegmentId);
            if (index != null) {
                index.close();
                this.cacheManager.unregister(index);
            }

            return index != null;
        }
    }

    //endregion
}
