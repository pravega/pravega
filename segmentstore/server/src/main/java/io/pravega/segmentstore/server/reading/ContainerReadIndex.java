/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.CacheFactory;
import io.pravega.segmentstore.storage.ReadOnlyStorage;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

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
@ThreadSafe
public class ContainerReadIndex implements ReadIndex {
    //region Members

    private final String traceObjectId;
    @GuardedBy("lock")
    private final HashMap<Long, StreamSegmentReadIndex> readIndices;
    private final Object lock = new Object();
    private final Cache cache;
    private final ReadOnlyStorage storage;
    private final ScheduledExecutorService executor;
    private final ReadIndexConfig config;
    private final CacheManager cacheManager;
    @GuardedBy("lock")
    private ContainerMetadata metadata;
    @GuardedBy("lock")
    private ContainerMetadata preRecoveryMetadata;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerReadIndex class.
     *
     * @param config       Configuration for the ReadIndex.
     * @param metadata     The ContainerMetadata to attach to.
     * @param cacheFactory A CacheFactory that can be used to create Caches for storing data into.
     * @param storage      Storage to read data not in the ReadIndex from.
     * @param cacheManager The CacheManager to use for cache lifecycle management.
     * @param executor     An Executor to run async callbacks on.
     */
    public ContainerReadIndex(ReadIndexConfig config, ContainerMetadata metadata, CacheFactory cacheFactory, ReadOnlyStorage storage, CacheManager cacheManager, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(cacheFactory, "cacheFactory");
        Preconditions.checkNotNull(storage, "storage");
        Preconditions.checkNotNull(cacheManager, "cacheManager");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkArgument(!metadata.isRecoveryMode(), "Given ContainerMetadata is in Recovery Mode.");

        this.traceObjectId = String.format("ReadIndex[%s]", metadata.getContainerId());
        this.readIndices = new HashMap<>();
        this.config = config;
        this.cache = cacheFactory.getCache(String.format("Container_%d", metadata.getContainerId()));
        this.metadata = metadata;
        this.storage = storage;
        this.cacheManager = cacheManager;
        this.executor = executor;
        this.preRecoveryMetadata = null;
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            closeAllIndices(false); // Do not individually clear the cache; we are wiping it anyway when closing it.
            this.cache.close();
            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region ReadIndex Implementation

    @Override
    public void append(long streamSegmentId, long offset, byte[] data) throws StreamSegmentNotExistsException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        log.debug("{}: append (StreamSegmentId = {}, Offset = {}, DataLength = {}).", this.traceObjectId, streamSegmentId, offset, data.length);

        // Append the data to the StreamSegment Index. It performs further validation with respect to offsets, etc.
        StreamSegmentReadIndex index = getOrCreateIndex(streamSegmentId);
        Exceptions.checkArgument(!index.isMerged(), "streamSegmentId", "StreamSegment is merged. Cannot append to it anymore.");
        index.append(offset, data);
    }

    @Override
    public void beginMerge(long targetStreamSegmentId, long offset, long sourceStreamSegmentId) throws StreamSegmentNotExistsException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        log.debug("{}: beginMerge (TargetId = {}, Offset = {}, SourceId = {}).", this.traceObjectId, targetStreamSegmentId, offset, sourceStreamSegmentId);

        StreamSegmentReadIndex targetIndex = getOrCreateIndex(targetStreamSegmentId);
        StreamSegmentReadIndex sourceIndex = getOrCreateIndex(sourceStreamSegmentId);
        Exceptions.checkArgument(!targetIndex.isMerged(), "targetStreamSegmentId", "StreamSegment is merged. Cannot access it anymore.");
        Exceptions.checkArgument(!sourceIndex.isMerged(), "sourceStreamSegmentId", "StreamSegment is merged. Cannot access it anymore.");
        targetIndex.beginMerge(offset, sourceIndex);
        sourceIndex.markMerged();
    }

    @Override
    public void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId) throws StreamSegmentNotExistsException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        log.debug("{}: completeMerge (TargetId = {}, SourceId = {}.", this.traceObjectId, targetStreamSegmentId, sourceStreamSegmentId);

        SegmentMetadata sourceMetadata;
        synchronized (this.lock) {
            sourceMetadata = this.metadata.getStreamSegmentMetadata(sourceStreamSegmentId);
        }

        Preconditions.checkState(sourceMetadata != null, "No Metadata found for Segment Id %s.", sourceStreamSegmentId);

        StreamSegmentReadIndex targetIndex = getOrCreateIndex(targetStreamSegmentId);
        targetIndex.completeMerge(sourceMetadata);
        synchronized (this.lock) {
            // Do not clear the Cache after merger - we are reusing the cache entries from the source index in the target one.
            closeIndex(sourceStreamSegmentId, false);
        }
    }

    @Override
    public ReadResult read(long streamSegmentId, long offset, int maxLength, Duration timeout) throws StreamSegmentNotExistsException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        log.debug("{}: read (StreamSegmentId = {}, Offset = {}, MaxLength = {}).", this.traceObjectId, streamSegmentId, offset, maxLength);

        StreamSegmentReadIndex index = getOrCreateIndex(streamSegmentId);
        Exceptions.checkArgument(!index.isMerged(), "streamSegmentId", "StreamSegment is merged. Cannot access it anymore.");
        return index.read(offset, maxLength, timeout);
    }

    @Override
    public InputStream readDirect(long streamSegmentId, long offset, int length) throws StreamSegmentNotExistsException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        log.debug("{}: readDirect (StreamSegmentId = {}, Offset = {}, Length = {}).", this.traceObjectId, streamSegmentId, offset, length);

        // Note that we do allow reading from partially merged StreamSegmentReadIndex. This should be ok since this is
        // an internal method, not meant to be used externally.
        StreamSegmentReadIndex index = getOrCreateIndex(streamSegmentId);
        return index.readDirect(offset, length);
    }

    @Override
    public void triggerFutureReads(Collection<Long> streamSegmentIds) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        log.debug("{}: triggerFutureReads (StreamSegmentIds = {}).", this.traceObjectId, streamSegmentIds);

        HashSet<Long> missingIds = new HashSet<>();
        for (long segmentId : streamSegmentIds) {
            StreamSegmentReadIndex index = getIndex(segmentId);
            if (index == null) {
                synchronized (this.lock) {
                    if (this.metadata.getStreamSegmentMetadata(segmentId) == null) {
                        missingIds.add(segmentId);
                    }
                }
                continue;
            }

            try {
                index.triggerFutureReads();
            } catch (ObjectClosedException ex) {
                // It is possible that between the time we got the pointer to the StreamSegmentReadIndex and when we got
                // to invoking triggerFutureReads, the StreamSegmentReadIndex has already been closed. If this is the case,
                // ignore the error.
                // This is possible in the following scenario: for a Transaction, we have an Append/Seal, followed by a Merge;
                // the Append/Seal makes this index eligible for triggering future reads, and the Merge (once committed to Storage)
                // will close it. If the StorageWriter is sufficiently fast in comparison to the OperationProcessor callbacks
                // (which could be the case for in-memory unit tests), it may trigger this condition.
                if (getIndex(segmentId) != null) {
                    throw ex;
                } else {
                    log.debug("{}: triggerFutureReads: StreamSegmentId {} was skipped because it is no longer registered.",
                            this.traceObjectId, segmentId);
                }
            }
        }

        // Throw any exception at the end - we want to make sure at least the ones that did have a valid index entry got triggered.
        Exceptions.checkArgument(missingIds.size() == 0, "streamSegmentIds",
                "At least one StreamSegmentId does not exist in the metadata: %s", missingIds);
    }

    @Override
    public void clear() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(isRecoveryMode(), "Read Index is not in recovery mode. Cannot clear ReadIndex.");
        closeAllIndices(true);
        log.info("{}: Cleared.", this.traceObjectId);
    }

    @Override
    public void cleanup(Collection<Long> segmentIds) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        List<Long> removed = new ArrayList<>();
        List<Long> notRemoved = new ArrayList<>();
        synchronized (this.lock) {
            if (segmentIds == null) {
                segmentIds = new ArrayList<>(this.readIndices.keySet());
            }

            for (long streamSegmentId : segmentIds) {
                SegmentMetadata segmentMetadata = this.metadata.getStreamSegmentMetadata(streamSegmentId);
                boolean wasRemoved = false;
                if (segmentMetadata == null || segmentMetadata.isDeleted() || !segmentMetadata.isActive()) {
                    wasRemoved = closeIndex(streamSegmentId, true);
                }

                if (wasRemoved) {
                    removed.add(streamSegmentId);
                } else {
                    notRemoved.add(streamSegmentId);
                }
            }
        }

        if (notRemoved.size() > 0) {
            log.debug("{}: Unable to clean up ReadIndex for Segments {} because no such index exists or the Segments are not deleted.", this.traceObjectId, notRemoved);
        }

        log.info("{}: Cleaned up ReadIndices for deleted Segments {}.", this.traceObjectId, removed);
    }

    @Override
    public void enterRecoveryMode(ContainerMetadata recoveryMetadataSource) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(!isRecoveryMode(), "Read Index is already in recovery mode.");
        Preconditions.checkNotNull(recoveryMetadataSource, "recoveryMetadataSource");
        Preconditions.checkArgument(recoveryMetadataSource.isRecoveryMode(), "Given ContainerMetadata is not in recovery mode.");

        // Swap metadata with recovery metadata (but still keep track of recovery metadata.
        synchronized (this.lock) {
            Preconditions.checkArgument(this.metadata.getContainerId() == recoveryMetadataSource.getContainerId(),
                    "Given ContainerMetadata refers to a different container than this ReadIndex.");
            assert this.preRecoveryMetadata == null
                    : "preRecoveryMetadata is not null, which should not happen unless we already are in recovery mode";
            this.preRecoveryMetadata = this.metadata;
            this.metadata = recoveryMetadataSource;
        }

        log.info("{} Enter RecoveryMode.", this.traceObjectId);
        clear();
    }

    @Override
    public void exitRecoveryMode(boolean successfulRecovery) throws DataCorruptionException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.isRecoveryMode(), "Read Index is not in recovery mode.");

        synchronized (this.lock) {
            assert this.preRecoveryMetadata != null
                    : "preRecoveryMetadata is null, which should only be the case when we are not in recovery mode";
            Preconditions.checkState(!this.preRecoveryMetadata.isRecoveryMode(),
                    "Cannot take ReadIndex out of recovery: ContainerMetadata is still in recovery mode.");

            if (successfulRecovery) {
                // Validate that the metadata has been properly recovered and that we are still in sync with it.
                for (Map.Entry<Long, StreamSegmentReadIndex> e : this.readIndices.entrySet()) {
                    SegmentMetadata metadata = this.preRecoveryMetadata.getStreamSegmentMetadata(e.getKey());
                    if (metadata == null) {
                        throw new DataCorruptionException(
                                String.format("ContainerMetadata has no knowledge of StreamSegment Id %s.", e.getKey()));
                    }

                    e.getValue().exitRecoveryMode(metadata);
                }
            } else {
                // Recovery was unsuccessful. Clear the contents of the ReadIndex to avoid further issues.
                clear();
            }

            this.metadata = this.preRecoveryMetadata;
            this.preRecoveryMetadata = null;
        }

        log.info("{} Exit RecoveryMode.", this.traceObjectId);
    }

    @Override
    public double getCacheUtilization() {
        return this.cacheManager.getCacheUtilization();
    }

    //endregion

    //region Helpers

    private boolean isRecoveryMode() {
        synchronized (this.lock) {
            return this.preRecoveryMetadata != null;
        }
    }

    /**
     * Gets a reference to the existing StreamSegmentRead index for the given StreamSegment Id.
     *
     * @param streamSegmentId The Id of the StreamSegment whose ReadIndex to get.
     */
    private StreamSegmentReadIndex getIndex(long streamSegmentId) {
        synchronized (this.lock) {
            return this.readIndices.getOrDefault(streamSegmentId, null);
        }
    }

    /**
     * Gets a reference to the existing StreamSegmentRead index for the given StreamSegment Id. Creates a new one if
     * necessary.
     *
     * @param streamSegmentId    The Id of the StreamSegment whose ReadIndex to get.
     */
    private StreamSegmentReadIndex getOrCreateIndex(long streamSegmentId) throws StreamSegmentNotExistsException {
        StreamSegmentReadIndex index;
        synchronized (this.lock) {
            // Try to see if we have the index already in memory.
            index = getIndex(streamSegmentId);
            if (index == null) {
                // We don't have it, create one.
                SegmentMetadata segmentMetadata = this.metadata.getStreamSegmentMetadata(streamSegmentId);
                Exceptions.checkArgument(segmentMetadata != null, "streamSegmentId",
                        "StreamSegmentId %s does not exist in the metadata.", streamSegmentId);
                if (segmentMetadata.isDeleted()) {
                    throw new StreamSegmentNotExistsException(segmentMetadata.getName());
                }

                index = new StreamSegmentReadIndex(this.config, segmentMetadata, this.cache, this.storage, this.executor, isRecoveryMode());
                this.cacheManager.register(index);
                this.readIndices.put(streamSegmentId, index);
            }
        }

        return index;
    }

    @GuardedBy("lock")
    private boolean closeIndex(long streamSegmentId, boolean cleanCache) {
        StreamSegmentReadIndex index = this.readIndices.remove(streamSegmentId);
        if (index != null) {
            this.cacheManager.unregister(index);
            index.close(cleanCache);
        }

        return index != null;
    }

    private void closeAllIndices(boolean cleanCache) {
        synchronized (this.lock) {
            val segmentIds = new ArrayList<Long>(this.readIndices.keySet());
            segmentIds.forEach(segmentId -> closeIndex(segmentId, cleanCache));
            this.readIndices.clear();
        }
    }

    //endregion
}
