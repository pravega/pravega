/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.HashHelper;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.CollectionHelpers;
import io.pravega.common.util.Retry;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.server.reading.AsyncReadResultHandler;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import io.pravega.segmentstore.server.reading.StreamSegmentStorageReader;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Attribute Index for a single Segment.
 */
@Slf4j
class SegmentAttributeIndex implements AttributeIndex, CacheManager.Client, AutoCloseable {
    //region Members

    /**
     * For Attribute Segment Appends, we want to write conditionally based on the offset, and retry the operation if
     * it failed for that reason. That guarantees that we won't be losing any data if we get concurrent calls to put().
     */
    private static final Retry.RetryAndThrowBase<Exception> APPEND_RETRY = Retry
            .withExpBackoff(10, 2, 10, 1000)
            .retryingOn(BadOffsetException.class)
            .throwingOn(Exception.class);

    /**
     * Calls to get() and put() can execute concurrently, which means we can have concurrent reads and writes from/to the
     * Attribute Segment, which in turn means we can truncate the segment while reading from it. We need to retry reads
     * if we stumble upon a segment truncation.
     */
    private static final Retry.RetryAndThrowBase<Exception> READ_RETRY = Retry
            .withExpBackoff(10, 2, 10, 1000)
            .retryingOn(StreamSegmentTruncatedException.class)
            .throwingOn(Exception.class);

    /**
     * The number of buckets to hash Attributes into with respect to caching. Since each cache operation involves some sort
     * of marshalling and serialization, the higher the number of buckets the less of those we'll need to make.
     */
    private static final int CACHE_BUCKETS = 64;
    private static final HashHelper HASH = HashHelper.seededWith(SegmentAttributeIndex.class.getName());

    private final SegmentMetadata segmentMetadata;
    private final AtomicReference<AttributeSegment> attributeSegment;
    private final Storage storage;
    private final OperationLog operationLog;
    private final Cache cache;
    @GuardedBy("cacheEntries")
    private int currentCacheGeneration;
    @GuardedBy("cacheEntries")
    private final CacheEntry[] cacheEntries;
    private final AttributeIndexConfig config;
    private final ScheduledExecutorService executor;
    private final String traceObjectId;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor & Initialization

    /**
     * Creates a new instance of the SegmentAttributeIndex class.
     *
     * @param segmentMetadata The SegmentMetadata of the Segment whose attributes we want to manage.
     * @param storage         A Storage adapter which can be used to access the Attribute Segment.
     * @param operationLog    An OperationLog that can be used to atomically update attributes for the main Segment.
     * @param cache           The Cache to use.
     * @param config          Attribute Index Configuration.
     * @param executor        An Executor to run async tasks.
     */
    SegmentAttributeIndex(SegmentMetadata segmentMetadata, Storage storage, OperationLog operationLog, Cache cache,
                          AttributeIndexConfig config, ScheduledExecutorService executor) {
        this.segmentMetadata = Preconditions.checkNotNull(segmentMetadata, "segmentMetadata");
        this.storage = Preconditions.checkNotNull(storage, "storage");
        this.operationLog = Preconditions.checkNotNull(operationLog, "operationLog");
        this.cache = Preconditions.checkNotNull(cache, "cache");
        this.config = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.attributeSegment = new AtomicReference<>();
        this.traceObjectId = String.format("AttributeIndex[%s]", this.segmentMetadata.getId());
        this.cacheEntries = new CacheEntry[CACHE_BUCKETS];
        this.closed = new AtomicBoolean();
    }

    /**
     * Initializes the SegmentAttributeIndex by inspecting the AttributeSegmentFile and creating it if needed.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation has succeeded.
     */
    CompletableFuture<Void> initialize(Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        String attributeSegmentName = StreamSegmentNameUtils.getAttributeSegmentName(this.segmentMetadata.getName());
        Preconditions.checkState(this.attributeSegment.get() == null, "SegmentAttributeIndex is already initialized.");
        // Attempt to open the Attribute Segment; if it does not exist yet then create it.
        return Futures
                .exceptionallyComposeExpecting(
                        this.storage.openWrite(attributeSegmentName)
                                .thenComposeAsync(handle -> this.storage
                                        .getStreamSegmentInfo(attributeSegmentName, timer.getRemaining())
                                        .thenAccept(si -> this.attributeSegment.set(new AttributeSegment(handle, si.getLength()))), this.executor),
                        ex -> ex instanceof StreamSegmentNotExistsException,
                        () -> this.storage.create(attributeSegmentName, this.config.getAttributeSegmentRollingPolicy(), timer.getRemaining())
                                .thenComposeAsync(si -> this.storage.openWrite(attributeSegmentName)
                                        .thenAccept(handle -> this.attributeSegment.set(new AttributeSegment(handle, si.getLength()))), this.executor)
                )
                .thenRun(() -> log.debug("{}: Initialized (Attribute Segment Length = {}).", this.traceObjectId, this.attributeSegment.get().getLength()));
    }

    /**
     * Deletes all the Attribute data associated with the given Segment.
     *
     * @param segmentName The name of the Segment whose attribute data should be deleted.
     * @param storage     A Storage Adapter to execute the deletion on.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation finished successfully.
     */
    static CompletableFuture<Void> delete(String segmentName, Storage storage, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        String attributeSegmentName = StreamSegmentNameUtils.getAttributeSegmentName(segmentName);
        return Futures.exceptionallyExpecting(
                storage.openWrite(attributeSegmentName)
                       .thenCompose(handle -> storage.delete(handle, timer.getRemaining())),
                ex -> ex instanceof StreamSegmentNotExistsException,
                null);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        // Quick close (no cache cleanup) this should be used only in case of container shutdown, when the cache will
        // be erased anyway.
        close(false);
    }

    /**
     * Closes the SegmentAttributeIndex and optionally cleans the cache.
     *
     * @param cleanCache If true, the Cache will be cleaned up of all entries pertaining to this Index. If false, the
     *                   Cache will not be touched.
     */
    void close(boolean cleanCache) {
        if (!this.closed.getAndSet(true)) {
            // Close storage reader (and thus cancel those reads).
            if (cleanCache) {
                this.executor.execute(() -> {
                    removeAllCacheEntries();
                    log.info("{}: Closed.", this.traceObjectId);
                });
            } else {
                log.info("{}: Closed (no cache cleanup).", this.traceObjectId);
            }
        }
    }

    /**
     * Removes all entries from the cache.
     */
    @VisibleForTesting
    void removeAllCacheEntries() {
        List<CacheEntry> entries;
        synchronized (this.cacheEntries) {
            entries = Arrays.stream(this.cacheEntries).filter(Objects::nonNull).collect(Collectors.toList());
            Arrays.fill(this.cacheEntries, null);
        }

        entries.forEach(CacheEntry::clear);
        log.info("{}: Cleared all cache entries ({}).", this.traceObjectId, entries.size());
    }

    //endregion

    //region CacheManager.Client implementation

    @Override
    public CacheManager.CacheStatus getCacheStatus() {
        int minGen = 0;
        int maxGen = 0;
        long size = 0;
        synchronized (this.cacheEntries) {
            for (CacheEntry e : this.cacheEntries) {
                if (e != null) {
                    int g = e.getGeneration();
                    minGen = Math.min(minGen, g);
                    maxGen = Math.max(maxGen, g);
                    size += e.getSize();
                }
            }
        }

        return new CacheManager.CacheStatus(size, minGen, maxGen);
    }

    @Override
    public long updateGenerations(int currentGeneration, int oldestGeneration) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        // Remove those entries that have a generation below the oldest permissible one.
        long sizeRemoved = 0;
        synchronized (this.cacheEntries) {
            this.currentCacheGeneration = currentGeneration;
            for (int i = 0; i < this.cacheEntries.length; i++) {
                CacheEntry e = this.cacheEntries[i];
                if (e != null && e.getGeneration() < oldestGeneration) {
                    this.cache.remove(e.getKey());
                    sizeRemoved += e.getSize();
                    this.cacheEntries[i] = null;
                }
            }
        }

        return sizeRemoved;
    }

    //endregion

    //region AttributeIndex implementation

    @Override
    public CompletableFuture<Void> put(UUID key, Long value, Duration timeout) {
        return put(Collections.singletonMap(key, value), timeout);
    }

    @Override
    public CompletableFuture<Void> put(Map<UUID, Long> values, Duration timeout) {
        ensureInitialized();
        Preconditions.checkNotNull(values, "values");
        if (values.size() == 0) {
            // Nothing to do.
            return CompletableFuture.completedFuture(null);
        } else {
            AttributeCollection c = new AttributeCollection(values);

            // Check if we are overdue for a snapshot. If so, create one while including the new values (which should prevent
            // us from having to write them separately).
            CompletableFuture<WriteInfo> result = shouldSnapshot()
                    ? createSnapshot(c, false, timeout)
                    : appendConditionally(() -> CompletableFuture.completedFuture(serialize(c)), new TimeoutTimer(timeout));

            // Update the cache after the operation succeeds.
            return result.thenAcceptAsync(writeInfo -> updateCache(c, writeInfo.getEndOffset()), this.executor);
        }
    }

    @Override
    public CompletableFuture<Map<UUID, Long>> get(Collection<UUID> keys, Duration timeout) {
        ensureInitialized();
        if (keys.size() == 0) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        Map<UUID, Long> cachedValues = getFromCache(keys);
        if (cachedValues.size() == keys.size()) {
            // Cache hit.
            return CompletableFuture.completedFuture(cachedValues);
        }

        // This will process all attributes anyway so we need not bother with partially filling the result and then selectively
        // picking our values from the result. We'll re-cache all attributes at this point.
        return readAllSinceLastSnapshot(true, timeout)
                .thenApply(c -> {
                    ImmutableMap.Builder<UUID, Long> b = ImmutableMap.builder();
                    keys.forEach(attributeId -> {
                        long value = c.attributes.getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE);
                        if (value != Attributes.NULL_ATTRIBUTE_VALUE) {
                            b.put(attributeId, value);
                        }
                    });
                    return b.build();
                });
    }

    @Override
    public CompletableFuture<Long> get(UUID key, Duration timeout) {
        ensureInitialized();
        Map<UUID, Long> cachedValues = getFromCache(Collections.singleton(key));
        if (!cachedValues.isEmpty()) {
            // Cache hit.
            return CompletableFuture.completedFuture(cachedValues.get(key));
        }

        return readAllSinceLastSnapshot(true, timeout)
                .thenApply(c -> c.attributes.get(key));
    }

    @Override
    public CompletableFuture<Void> remove(UUID key, Duration timeout) {
        return remove(Collections.singleton(key), timeout);
    }

    @Override
    public CompletableFuture<Void> remove(Collection<UUID> keys, Duration timeout) {
        Preconditions.checkNotNull(keys, "keys");
        return put(keys.stream().collect(Collectors.toMap(key -> key, key -> Attributes.NULL_ATTRIBUTE_VALUE)), timeout);
    }

    @Override
    public CompletableFuture<Void> seal(Duration timeout) {
        ensureInitialized();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return Futures.exceptionallyExpecting(
                createSnapshot(new AttributeCollection(), true, timer.getRemaining())
                        .thenComposeAsync(v -> this.storage.seal(this.attributeSegment.get().handle, timer.getRemaining()), this.executor)
                        .thenRun(() -> log.info("{}: Sealed (Length = {}).", this.traceObjectId, this.attributeSegment.get().getLength())),
                ex -> ex instanceof StreamSegmentSealedException,
                null);
    }

    //endregion

    //region Operations

    /**
     * Determines if we should generate a Snapshot based on the current state of the Attribute Segment.
     *
     * @return True if we should generate a Snapshot, false otherwise.
     */
    @VisibleForTesting
    boolean shouldSnapshot() {
        long lastSnapshotEndOffset = getLastSnapshotOffset() + getLastSnapshotLength();
        return this.attributeSegment.get().getLength() - lastSnapshotEndOffset >= this.config.getSnapshotTriggerSize();
    }

    /**
     * Reads all attributes beginning with the last recorded snapshot until the current end of the Attribute Segment.
     *
     * @param cacheValues If true, the read values will be cached.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the Attributes as they should be when reaching the
     * end of the Attribute Segment.
     */
    private CompletableFuture<AttributeCollection> readAllSinceLastSnapshot(boolean cacheValues, Duration timeout) {
        AtomicLong lastReadOffset = new AtomicLong();
        CompletableFuture<AttributeCollection> result = READ_RETRY.runAsync(() -> {
            ensureMainSegmentExists();
            long lastSnapshotOffset = getLastSnapshotOffset();
            int readLength = (int) Math.min(Integer.MAX_VALUE, this.attributeSegment.get().getLength() - lastSnapshotOffset);
            lastReadOffset.set(lastSnapshotOffset + readLength);
            CompletableFuture<AttributeCollection> r = new CompletableFuture<>();
            if (readLength == 0) {
                // Nothing to read.
                r.complete(new AttributeCollection());
            } else {
                AsyncReadResultProcessor.process(
                        StreamSegmentStorageReader.read(this.attributeSegment.get().handle, lastSnapshotOffset, readLength, this.config.getReadBlockSize(), this.storage),
                        new AttributeSegmentReader(r, timeout),
                        this.executor);
            }
            return r;
        }, this.executor);

        if (cacheValues) {
            result = result.thenApplyAsync(r -> {
                updateCache(r, lastReadOffset.get());
                return r;
            }, this.executor);
        }

        return Futures.exceptionallyCompose(result,
                ex -> {
                    ex = Exceptions.unwrap(ex);
                    if (ex instanceof IOException || ex instanceof StreamSegmentTruncatedException) {
                        ex = new DataCorruptionException(
                                String.format("Unable to parse AttributeSegment. LastSnapshot = (Offset=%d, Length=%d), Known Segment Length = %d.",
                                        getLastSnapshotOffset(), getLastSnapshotLength(), this.attributeSegment.get().getLength()), ex);
                    }
                    return Futures.failedFuture(ex);
                });
    }

    /**
     * Creates a new Snapshot, writes it to the AttributeSegment and updates the main Segment's attributes with its location.
     *
     * @param newAttributes Any new Attributes to include in this Snapshot that would not already be part of it.
     * @param mustComplete  Whether the update of the main Segment's attributes must execute successfully in order for this
     *                      operation to be considered complete. For auto-generated Snapshots, since the Snapshot itself is
     *                      already written to the AttributeSegment, it is not mandatory for the OperationLog Add &
     *                      Attribute Segment Truncation operations to complete, as there will be no data loss. However, for
     *                      Snapshots generated as part of Seals, all of those must complete in order for the operation to
     *                      be considered successful.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation has completed successfully. It
     * will contain information about the Storage write pertaining to the Snapshot write.
     */
    private CompletableFuture<WriteInfo> createSnapshot(AttributeCollection newAttributes, boolean mustComplete, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // The serialization may be invoked multiple times, based on whether the appendConditionally() requires a retry.
        Supplier<CompletableFuture<ArrayView>> s = () ->
                readAllSinceLastSnapshot(false, timer.getRemaining())
                        .thenApplyAsync(c -> {
                            c.mergeWith(newAttributes);
                            return serialize(c);
                        }, this.executor);
        return appendConditionally(s, timer)
                .thenComposeAsync(w -> updateStatePostSnapshot(w, mustComplete, timer), this.executor);
    }

    /**
     * Updates the system's state post a successful Snapshot write to the Attribute Segment.
     *
     * @param writeInfo    Information about the data written as part of the Snapshot.
     * @param mustComplete Whether the operation must complete. Refer to createSnapshot's mustComplete for more details.
     * @param timer        Timer for the operation (used for timeouts).
     * @return A CompletableFuture that, when completed, will indicate that the operation has completed successfully. It
     * will contain the value of the writeInfo argument passed into this method.
     */
    private CompletableFuture<WriteInfo> updateStatePostSnapshot(WriteInfo writeInfo, boolean mustComplete, TimeoutTimer timer) {
        log.debug("{}: Snapshot serialized to attribute segment ({}).", this.traceObjectId, writeInfo);
        UpdateAttributesOperation op = new UpdateAttributesOperation(this.segmentMetadata.getId(), Arrays.asList(
                new AttributeUpdate(Attributes.LAST_ATTRIBUTE_SNAPSHOT_OFFSET, AttributeUpdateType.ReplaceIfGreater, writeInfo.offset),
                new AttributeUpdate(Attributes.LAST_ATTRIBUTE_SNAPSHOT_LENGTH, AttributeUpdateType.Replace, writeInfo.length)));

        CompletableFuture<Void> result = this.operationLog
                .add(op, timer.getRemaining())
                .handleAsync((v, ex) -> {
                    if (ex != null) {
                        // If we are unable to update the main segment attributes due to the segment having been sealed,
                        // merged or deleted, do not do anything else.
                        ex = Exceptions.unwrap(ex);
                        if (ex instanceof StreamSegmentMergedException
                                || ex instanceof StreamSegmentSealedException
                                || ex instanceof StreamSegmentNotExistsException) {
                            log.warn("{}: Snapshot serialized to attribute segment, but failed to update snapshot location due to {}.",
                                    this.traceObjectId, ex.toString());
                            return CompletableFuture.<Void>completedFuture(null);
                        } else {
                            return Futures.<Void>failedFuture(ex);
                        }
                    }

                    log.debug("{}: Snapshot location updated in main segment's metadata ({}).", this.traceObjectId, writeInfo);
                    ensureMainSegmentExists();
                    return this.storage.truncate(this.attributeSegment.get().handle, writeInfo.offset, timer.getRemaining());
                }, this.executor)
                .thenCompose(f -> f);

        if (!mustComplete) {
            result = result.exceptionally(ex -> {
                // Failure to update snapshot location in the main segment's metadata or to truncate the Attribute Segment
                // is not a critical failure. The snapshot has been successfully written, and its data can be recovered.
                // If we bubble up this exception, the caller may think the data have not been written and retry, hence
                // unnecessarily write the data multiple times.
                // If we do get here, we'll simply have to process more data when we read and potentially have more
                // data than needed in the attribute segment, both of which will be fixed with the next snapshot attempt.
                ex = Exceptions.unwrap(ex);
                log.warn("{}: Snapshot serialized to attribute segment, but failed to update snapshot location or truncate Attribute Segment.", this.traceObjectId, ex);
                return null;
            });
        }
        return result.thenApply(v -> writeInfo);
    }

    /**
     * Appends the result of the given serialization function conditionally based on the current length of the Attribute Segment.
     * This method will retry the write subject to the APPEND_RETRY policy.
     *
     * @param getSerialization A Supplier that, when invoked, returns a CompletableFuture whose result will be the requested
     *                         serialization.
     * @param timer            Timer for the operation. Used for timeouts.
     * @return A CompletableFuture that, when completed, will contain information about the data that was just written.
     */
    private CompletableFuture<WriteInfo> appendConditionally(Supplier<CompletableFuture<ArrayView>> getSerialization, TimeoutTimer timer) {
        return APPEND_RETRY.runAsync(() -> appendConditionallyOnce(getSerialization, timer), this.executor);
    }

    /**
     * Appends the result of the given serialization function conditionally based on the current length of the Attribute Segment.
     * This method does not perform any retries.
     *
     * @param getSerialization A Supplier that, when invoked, returns a CompletableFuture whose result will be the requested
     *                         serialization.
     * @param timer            Timer for the operation. Used for timeouts.
     * @return A CompletableFuture that, when completed, will contain information about the data that was just written.
     */
    private CompletableFuture<WriteInfo> appendConditionallyOnce(Supplier<CompletableFuture<ArrayView>> getSerialization, TimeoutTimer timer) {
        // We want to make sure that the serialization we generate is accurate based on the state of the Attribute Segment.
        // This is to protect against potential corruptions due to concurrency: for example we picked data for a Snapshot,
        // then merged it with some other changes, then wrote it back - we want to ensure nobody else wrote anything in the meantime.
        // As such, we need to do a conditional append keyed on the length of the Attribute Segment. Should there be
        // a concurrent change, we will need to re-generate the serialization in order to guarantee that we always write
        // the latest data.
        AttributeSegment as = this.attributeSegment.get();
        ensureMainSegmentExists();

        long offset = as.getLength();
        return getSerialization.get().thenComposeAsync(data ->
                Futures.exceptionallyCompose(
                        this.storage
                                .write(as.handle, offset, data.getReader(), data.getLength(), timer.getRemaining())
                                .thenApply(v -> {
                                    as.increaseLength(data.getLength());
                                    log.debug("{}: Wrote data ({}).", this.traceObjectId, data.getLength());
                                    return new WriteInfo(offset, data.getLength());
                                }),
                        ex -> {
                            if (Exceptions.unwrap(ex) instanceof BadOffsetException) {
                                return this.storage
                                        .getStreamSegmentInfo(this.attributeSegment.get().handle.getSegmentName(), timer.getRemaining())
                                        .thenCompose(si -> {
                                            as.setLength(si.getLength());
                                            return Futures.failedFuture(ex);
                                        });
                            } else {
                                return Futures.failedFuture(ex);
                            }
                        }));
    }

    /**
     * Serializes the given AttributeCollection.
     *
     * @param attributes The AttributeCollection to serialize.
     * @return An ArrayView representing the serialization.
     */
    @SneakyThrows(IOException.class)
    @VisibleForTesting
    ArrayView serialize(AttributeCollection attributes) {
        return AttributeCollection.SERIALIZER.serialize(attributes);
    }

    /**
     * Gets a SegmentHandle for the AttributeSegment.
     */
    @VisibleForTesting
    SegmentHandle getAttributeSegmentHandle() {
        return this.attributeSegment.get().handle;
    }

    /**
     * Gets the Offset of the Last Snapshot, as found in the main Segment's metadata.
     */
    private long getLastSnapshotOffset() {
        return this.segmentMetadata.getAttributes().getOrDefault(Attributes.LAST_ATTRIBUTE_SNAPSHOT_OFFSET, 0L);
    }

    /**
     * Gets the Length of the Last Snapshot, as found in the main Segment's metadata.
     */
    private int getLastSnapshotLength() {
        return (int) (long) this.segmentMetadata.getAttributes().getOrDefault(Attributes.LAST_ATTRIBUTE_SNAPSHOT_LENGTH, 0L);
    }

    /**
     * Verifies that the main Segment still exists (is not deleted). If it doesn't, it aborts the current operation by
     * throwing a StreamSegmentNotExistsException. If the main Segment is deleted, the owning SegmentContainer will be
     * deleting this Attribute Segment soon, so do not bother making any more changes to it.
     */
    @SneakyThrows(StreamSegmentNotExistsException.class)
    private void ensureMainSegmentExists() {
        if (this.segmentMetadata.isDeleted() || this.segmentMetadata.isMerged()) {
            log.info("{}: Main Segment ({}) is Deleted. Aborting operation.", this.traceObjectId, this.segmentMetadata.getName());
            throw new StreamSegmentNotExistsException(this.segmentMetadata.getName());
        }
    }

    /**
     * Updates the cache with the Attribute Values contained in the given collection, but only if the cache version is
     * lower than the given version.
     *
     * @param attributes The AttributeCollection that contains the updates to apply.
     * @param version    The update version.
     */
    private void updateCache(AttributeCollection attributes, long version) {
        if (attributes.attributes.isEmpty()) {
            return;
        }

        // Hash the attributes and map them to the appropriate Cache Entry.
        Map<Integer, List<Map.Entry<UUID, Long>>> hashedAttributes = hash(attributes.attributes);
        Map<CacheEntry, List<Map.Entry<UUID, Long>>> entryAttributes = new HashMap<>();
        int generation;
        synchronized (this.cacheEntries) {
            generation = this.currentCacheGeneration;
            hashedAttributes.forEach((bucket, values) -> {
                CacheEntry e = this.cacheEntries[bucket];
                if (e == null) {
                    e = new CacheEntry(bucket, generation);
                    this.cacheEntries[bucket] = e;
                }

                entryAttributes.put(e, values);
            });
        }

        // Outside of the main lock (since this may be time consuming), update each cache entry.
        entryAttributes.forEach((e, values) -> e.updateValues(values, version, generation));
    }

    /**
     * Fetches the attribute values from the Cache for those attributes ids in the given Collection that are present in the cache.
     *
     * @param attributeIds The Attribute Ids to query.
     * @return A Map containing those attribute values present in the Cache.
     */
    private Map<UUID, Long> getFromCache(Collection<UUID> attributeIds) {
        if (attributeIds.isEmpty()) {
            return Collections.emptyMap();
        }

        // Hash the attributes and map them to the appropriate Cache Entry.
        Map<Integer, List<UUID>> hashedAttributes = hash(attributeIds);
        Map<CacheEntry, List<UUID>> entryAttributes = new HashMap<>();
        int generation;
        synchronized (this.cacheEntries) {
            generation = this.currentCacheGeneration;
            hashedAttributes.forEach((bucket, ids) -> {
                CacheEntry e = this.cacheEntries[bucket];
                if (e != null) {
                    entryAttributes.put(e, ids);
                }
            });
        }

        // Outside of the main lock (since this may be time consuming), query each cache entry.
        Map<UUID, Long> result = new HashMap<>();
        entryAttributes.forEach((e, ids) -> e.fetchValues(ids, result, generation));
        return result;
    }

    private Map<Integer, List<Map.Entry<UUID, Long>>> hash(Map<UUID, Long> attributes) {
        return attributes.entrySet().stream()
                .collect(Collectors.groupingBy(e -> HASH.hashToBucket(e.getKey(), CACHE_BUCKETS)));
    }

    private Map<Integer, List<UUID>> hash(Collection<UUID> attributeIds) {
        return attributeIds.stream()
                .collect(Collectors.groupingBy(e -> HASH.hashToBucket(e, CACHE_BUCKETS)));
    }

    @VisibleForTesting
    Map<UUID, Integer> getBuckets(Collection<UUID> attributeIds) {
        return attributeIds.stream().collect(Collectors.toMap(id -> id, id -> HASH.hashToBucket(id, CACHE_BUCKETS)));
    }

    private void ensureInitialized() {
        Preconditions.checkState(this.attributeSegment.get() != null, "SegmentAttributeIndex is not initialized.");
    }

    //endregion

    //region AttributeSegmentReader

    /**
     * Async reader for the Attribute Segment from Storage.
     */
    @NotThreadSafe
    private static class AttributeSegmentReader implements AsyncReadResultHandler {
        private final ArrayList<InputStream> inputs = new ArrayList<>();
        private final CompletableFuture<AttributeCollection> result;
        private final AttributeCollection attributeCollection;
        private final TimeoutTimer timer;

        AttributeSegmentReader(CompletableFuture<AttributeCollection> result, Duration timeout) {
            this.attributeCollection = new AttributeCollection(null);
            this.result = result;
            this.timer = new TimeoutTimer(timeout);
        }

        @Override
        public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
            return true;
        }

        @Override
        public boolean processEntry(ReadResultEntry entry) {
            assert entry.getContent().isDone() : "received incomplete ReadResultEntry from reader";
            this.inputs.add(entry.getContent().join().getData());
            return true;
        }

        @Override
        public void processError(Throwable cause) {
            this.result.completeExceptionally(cause);
        }

        @Override
        public void processResultComplete() {
            Enumeration<InputStream> inputEnumeration = Collections.enumeration(inputs);
            try (SequenceInputStream inputStream = new SequenceInputStream(inputEnumeration)) {
                // Loop as long as the current InputStream has more elements or we have more input streams to process.
                // NOTE: SequenceInputStream.available() will return 0 if it is sitting on the current end of a member InputStream
                // so we cannot rely on that alone.
                while (inputEnumeration.hasMoreElements() || inputStream.available() > 0) {
                    AttributeCollection c = AttributeCollection.SERIALIZER.deserialize(inputStream);
                    this.attributeCollection.mergeWith(c);
                }
                this.result.complete(this.attributeCollection);
            } catch (Throwable ex) {
                processError(ex);
            }
        }

        @Override
        public Duration getRequestContentTimeout() {
            return this.timer.getRemaining();
        }
    }

    //endregion

    //region AttributeSegment

    /**
     * Metadata about the Attribute Segment.
     */
    private static class AttributeSegment {
        private final SegmentHandle handle;
        private final AtomicLong length;

        AttributeSegment(SegmentHandle handle, long initialLength) {
            this.handle = handle;
            this.length = new AtomicLong(initialLength);
        }

        long getLength() {
            return this.length.get();
        }

        void setLength(long value) {
            this.length.set(value);
        }

        void increaseLength(int delta) {
            Preconditions.checkArgument(delta >= 0, "increase must be non-negative");
            this.length.addAndGet(delta);
        }
    }

    //endregion

    //region AttributeCollection & Serializer

    /**
     * Collection of Attributes.
     */
    @Builder
    @VisibleForTesting
    static class AttributeCollection {
        private static final AttributeCollectionSerializer SERIALIZER = new AttributeCollectionSerializer();
        private final Map<UUID, Long> attributes;

        private AttributeCollection() {
            this.attributes = new HashMap<>();
        }

        private AttributeCollection(Map<UUID, Long> attributes) {
            this.attributes = attributes == null ? new HashMap<>() : attributes;
        }

        private void mergeWith(AttributeCollection other) {
            other.attributes.forEach((attributeId, value) -> {
                if (value == Attributes.NULL_ATTRIBUTE_VALUE) {
                    this.attributes.remove(attributeId);
                } else {
                    this.attributes.put(attributeId, value);
                }
            });
        }

        static class AttributeCollectionBuilder implements ObjectBuilder<AttributeCollection> {
        }

        static class AttributeCollectionSerializer extends VersionedSerializer.WithBuilder<AttributeCollection, AttributeCollectionBuilder> {
            @Override
            protected AttributeCollectionBuilder newBuilder() {
                return AttributeCollection.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(AttributeCollection c, RevisionDataOutput output) throws IOException {
                if (output.requiresExplicitLength()) {
                    output.length(output.getMapLength(c.attributes.size(), RevisionDataOutput.UUID_BYTES, Long.BYTES));
                }

                output.writeMap(c.attributes, RevisionDataOutput::writeUUID, RevisionDataOutput::writeLong);
            }

            private void read00(RevisionDataInput input, AttributeCollectionBuilder builder) throws IOException {
                builder.attributes(input.readMap(RevisionDataInput::readUUID, RevisionDataInput::readLong, HashMap::new));
            }
        }
    }

    //endregion

    //region WriteInfo

    /**
     * Information about a write to the AttributeSegment.
     */
    @Data
    @VisibleForTesting
    static class WriteInfo {
        private final long offset;
        private final int length;

        long getEndOffset() {
            return this.offset + this.length;
        }

        @Override
        public String toString() {
            return String.format("Offset=%d, Length=%d", this.offset, this.length);
        }
    }

    //endregion

    //region CacheEntry

    /**
     * An entry in the Cache to which one or more Attributes are mapped.
     */
    private class CacheEntry {
        /**
         * Id of the entry. This is used to lookup cached data in the Cache.
         */
        @Getter
        private final int entryId;
        @GuardedBy("this")
        private int generation;
        @GuardedBy("this")
        private int size;

        CacheEntry(int id, int currentGeneration) {
            this.entryId = id;
            this.generation = currentGeneration;
            this.size = 0;
        }

        /**
         * Gets a new CacheKey representing this Entry.
         */
        CacheKey getKey() {
            return new CacheKey(SegmentAttributeIndex.this.segmentMetadata.getId(), this.entryId);
        }

        /**
         * Gets a value representing the current Generation of this Cache Entry. This value is updated every time the
         * data behind this entry is modified or accessed.
         */
        synchronized int getGeneration() {
            return this.generation;
        }

        /**
         * Gets a value representing the size, in bytes, of the data behind this Cache Entry.
         */
        synchronized int getSize() {
            return this.size;
        }

        /**
         * Reads all the values from this Cache Entry for the given Attribute Ids into the given Map.
         *
         * @param attributeIds      The Attribute Ids to query.
         * @param result            A Map where to store the result. Only those Attribute Ids which have data in this entry
         *                          will be added here.
         * @param currentGeneration The current Cache Generation (from the Cache Manager). The internal generation will
         *                          only be updated if at least one Attribute Value is fetched (cache hit).
         */
        synchronized void fetchValues(Collection<UUID> attributeIds, Map<UUID, Long> result, int currentGeneration) {
            if (attributeIds.isEmpty()) {
                // Nothing to do.
                return;
            }

            byte[] data = SegmentAttributeIndex.this.cache.get(getKey());
            if (data != null && data.length > 0) {
                // The Cache Entry Serialization has the Attributes in sorted order (by Id), so the most efficient way of
                // searching the values is to do a binary search within the byte array.
                boolean anythingRead = CollectionHelpers.binarySearch(CacheEntryLayout.wrap(data), attributeIds, result);
                if (anythingRead) {
                    this.generation = currentGeneration;
                }
            }
        }

        /**
         * Updates the data in this Cache Entry with the new values. The new data will not overwrite the whole entry, rather
         * the new attribute values will be merged into the Entry, using the following scheme:
         * - Attributes that do not exist in attributeValues are left untouched.
         * - Attributes that do not exist in the entry but do exist in attributeValues are added.
         * - Attributes that exist in both the entry and attributeValues are only modified if their version is smaller than
         * the given version.
         *
         * @param attributeValues   The Attribute Values to add or update.
         * @param version           The current Cache Version. This is usually the last offset in the Attribute Segment
         *                          where the given set of Attribute Values are written.
         * @param currentGeneration The current Cache Generation (from the Cache Manager). The internal generation will
         *                          only be updated if at least one Attribute Value is updated.
         */
        synchronized void updateValues(Collection<Map.Entry<UUID, Long>> attributeValues, long version, int currentGeneration) {
            if (attributeValues.isEmpty()) {
                // Nothing to do.
                return;
            }

            // Fetch existing data.
            byte[] existingData = SegmentAttributeIndex.this.cache.get(getKey());
            Map<UUID, VersionedValue> values = existingData == null
                    ? new HashMap<>()
                    : CacheEntryLayout.wrap(existingData).getAllValues();

            // Merge new values.
            boolean changed = false;
            for (Map.Entry<UUID, Long> e : attributeValues) {
                VersionedValue existing = values.getOrDefault(e.getKey(), null);
                if (existing == null || existing.version < version) {
                    if (e.getValue() == Attributes.NULL_ATTRIBUTE_VALUE) {
                        values.remove(e.getKey());
                        changed |= existing != null;
                    } else {
                        // As per the CacheEntryLayout contract, we create a UUID with MSB set to Version and LSB set to Value
                        // in order to pass it down for serialization.
                        values.put(e.getKey(), new VersionedValue(version, e.getValue()));
                        changed |= existing == null || existing.value != e.getValue();
                    }
                }
            }

            // Serialize, but only if anything changed.
            if (changed) {
                byte[] newData = CacheEntryLayout.setValues(
                        existingData,
                        values.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).iterator(),
                        values.size());
                SegmentAttributeIndex.this.cache.insert(getKey(), newData);
                this.size = newData.length;
                this.generation = currentGeneration;
            }
        }

        /**
         * Removes all data associated with this cache entry.
         */
        synchronized void clear() {
            SegmentAttributeIndex.this.cache.remove(getKey());
            this.size = 0;
        }
    }

    //endregion
}
