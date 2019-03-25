/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.collect.Maps;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.ConditionalTableUpdateException;
import io.pravega.segmentstore.contracts.tables.KeyNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentNotEmptyException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.storage.CacheFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

/**
 * A cache-backed, in-memory Table Index representation with pass-through updates. Enables Conditional Updates and caching
 * of recently used Keys for faster access.
 */
@ThreadSafe
class ContainerKeyIndex implements AutoCloseable {
    //region Members

    @Getter
    private final IndexReader indexReader;
    private final ScheduledExecutorService executor;
    private final ContainerKeyCache cache;
    private final CacheManager cacheManager;
    private final MultiKeySequentialProcessor<Map.Entry<Long, UUID>> conditionalUpdateProcessor;
    private final RecoveryTracker recoveryTracker;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerKeyIndex class.
     *
     * @param containerId  Id of the SegmentContainer this instance is associated with.
     * @param cacheFactory A {@link CacheFactory} that can be used to create Cache instances.
     * @param cacheManager A {@link CacheManager} that can be used to manage Cache instances.
     * @param executor     Executor for async operations.
     */
    ContainerKeyIndex(int containerId, @NonNull CacheFactory cacheFactory, @NonNull CacheManager cacheManager, @NonNull ScheduledExecutorService executor) {
        this.cache = new ContainerKeyCache(containerId, cacheFactory);
        this.cacheManager = cacheManager;
        this.cacheManager.register(this.cache);
        this.executor = executor;
        this.indexReader = new IndexReader(executor);
        this.conditionalUpdateProcessor = new MultiKeySequentialProcessor<>(this.executor);
        this.recoveryTracker = new RecoveryTracker();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.conditionalUpdateProcessor.close();
            this.cacheManager.unregister(this.cache);
            this.cache.close();
            this.recoveryTracker.close();
        }
    }

    //endregion

    //region Operations

    /**
     * Executes the given action only if the Segment is empty.
     *
     * This is a conditional operation that will execute serially after all currently executing conditional updates on
     * the given Segment have completed. Any subsequent conditional updates on the given Segment will be blocked until
     * this action completes executing. Unconditional updates will not block the execution of this action, nor will they
     * be blocked by its execution.
     *
     * Similarly to conditional updates, this can be used to ensure consistency across the TableSegment, making sure that
     * the action only executes if a specific condition is met, while making sure no other consistency-related operations
     * may begin while it is running.
     *
     * A deadlock will occur if action is used to perform a conditional update on the same segment. If an update is
     * required, then an unconditional update should be executed via action.
     *
     * @param segment The Segment to perform the action on.
     * @param action  A Supplier that, when invoked, will begin executing the action. This returns a CompletableFuture
     *                that will indicate when the action completed.
     * @param <T>     Return type.
     * @return A CompletableFuture that, when completed normally, will indicate that the action was executed. If it failed,
     * or if the segment is not empty, it will be failed with the appropriate exception. Notable exceptions:
     * <ul>
     * <li>{@link TableSegmentNotEmptyException} If the Segment is not empty.
     * </ul>
     */
    <T> CompletableFuture<T> executeIfEmpty(DirectSegmentAccess segment, Supplier<CompletableFuture<T>> action) {
        return this.recoveryTracker.waitIfNeeded(segment, () -> this.conditionalUpdateProcessor.addWithFilter(
                conditionKey -> conditionKey.getKey() == segment.getSegmentId(),
                () -> {
                    SegmentProperties sp = segment.getInfo();
                    long entryCount = this.indexReader.getBucketCount(sp) + this.cache.getBucketCountDelta(segment.getSegmentId());
                    if (entryCount <= 0) {
                        // Segment is empty.
                        return action.get();
                    } else {
                        // Segment is not empty.
                        return Futures.failedFuture(new TableSegmentNotEmptyException(sp.getName()));
                    }
                }
        ));
    }

    /**
     * Finds the Last Bucket Offsets for the given KeyHashes.
     *
     * @param segment Segment to look up Bucket Offsets for.
     * @param hashes  A Collection of Key Hashes to identify the Buckets.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the sought Offsets indexed by their corresponding
     * Key Hash. If a particular bucket does not exist, its corresponding Key Hash will have a {@link TableKey#NOT_EXISTS}
     * associated with it.
     */
    CompletableFuture<Map<UUID, Long>> getBucketOffsets(DirectSegmentAccess segment, Collection<UUID> hashes, TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        if (hashes.isEmpty()) {
            // Nothing to search.
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        // Find those keys which already exist in the cache. The same hash may occur multiple times, but this process
        // helps dedupe it.
        val result = new HashMap<UUID, Long>();
        val toLookup = new ArrayList<UUID>();
        for (UUID hash : hashes) {
            if (result.containsKey(hash)) {
                // This KeyHash has already been processed.
                continue;
            }

            val existingValue = this.cache.get(segment.getSegmentId(), hash);
            if (existingValue == null) {
                // Key Hash does not exist in the cache (it may or may not exist at all). Add a placeholder and keep
                // track of it so we can look it up.
                result.put(hash, TableKey.NOT_EXISTS);
                toLookup.add(hash);
            } else if (!existingValue.isRemoval()) {
                // Key Hash exists.
                result.put(hash, existingValue.getSegmentOffset());
            } else {
                long backpointerOffset = this.cache.getBackpointer(segment.getSegmentId(), existingValue.getSegmentOffset());
                if (backpointerOffset < 0) {
                    // Key Hash does not exist in the cache. Queue it up for lookup.
                    result.put(hash, TableKey.NOT_EXISTS);
                    toLookup.add(hash);
                } else {
                    // Key Hash (Table Bucket) has been created/updated recently, however it also had a removal, as such
                    // we are pointing to the last update, but there are other entries for this Bucket that may be of interest
                    // to the caller.
                    result.put(hash, existingValue.getSegmentOffset());
                }
            }
        }

        if (toLookup.isEmpty()) {
            // Full cache hit.
            return CompletableFuture.completedFuture(result);
        } else {
            // Fetch information for missing hashes.
            return this.recoveryTracker.waitIfNeeded(segment, () -> getBucketOffsetFromSegment(segment, result, toLookup, timer));
        }
    }

    /**
     * Finds the Bucket Offset for the given Key Hash directly from the index (excluding the cache). If the index contains
     * an updated offset for this Bucket (i.e., as a result of a compaction), the cache will be automatically updated in
     * the process. If the index contains an obsolete offset for this Bucket (compared to the cache), the cache value
     * will be returned.
     *
     * @param segment Segment to look up the Bucket Offset for.
     * @param keyHash The Key Hash to look up.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the sought offset. If the  bucket does not exist,
     * it will contain {@link TableKey#NOT_EXISTS}.
     * associated with it.
     */
    CompletableFuture<Long> getBucketOffsetDirect(DirectSegmentAccess segment, UUID keyHash, TimeoutTimer timer) {
        // Get the bucket offset from the segment, which will update the cache if actually newer.
        return this.recoveryTracker.waitIfNeeded(segment,
                () -> getBucketOffsetFromSegment(segment, Collections.synchronizedMap(new HashMap<>()), Collections.singleton(keyHash), timer)
                        .thenApply(result -> result.get(keyHash)));
    }

    private CompletableFuture<Map<UUID, Long>> getBucketOffsetFromSegment(DirectSegmentAccess segment, Map<UUID, Long> result,
                                                                          Collection<UUID> toLookup, TimeoutTimer timer) {
        return this.indexReader
                .locateBuckets(segment, toLookup, timer)
                .thenApplyAsync(bucketsByHash -> {
                    for (val e : bucketsByHash.entrySet()) {
                        UUID keyHash = e.getKey();
                        TableBucket bucket = e.getValue();

                        // Cache the bucket's location, but only if its path is complete.
                        if (bucket.exists()) {
                            // Update the cache information.
                            long highestOffset = this.cache.includeExistingKey(
                                    segment.getSegmentId(), keyHash, bucket.getSegmentOffset());
                            result.put(keyHash, highestOffset);
                        } else {
                            // Inexistent bucket. What we are looking for does not exist. Do not update the information
                            // in the cache as this would have the potential to fill up the cache with useless keys
                            // if the application requests a lot of them (excellent DoS opportunity!).
                            result.put(keyHash, TableKey.NOT_EXISTS);
                        }
                    }

                    return result;
                }, this.executor);
    }

    /**
     * Looks up a Backpointer offset.
     *
     * @param segment A DirectSegmentAccess providing access to the Segment to search in.
     * @param offset  The offset to find a backpointer from.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the backpointer offset, or -1 if no such pointer exists.
     */
    CompletableFuture<Long> getBackpointerOffset(DirectSegmentAccess segment, long offset, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        // First check the index tail cache.
        long cachedBackpointer = this.cache.getBackpointer(segment.getSegmentId(), offset);
        if (cachedBackpointer >= 0) {
            return CompletableFuture.completedFuture(cachedBackpointer);
        }

        if (offset <= this.cache.getSegmentIndexOffset(segment.getSegmentId())) {
            // The sought source offset is already indexed; do not bother with waiting for recovery.
            return this.indexReader.getBackpointerOffset(segment, offset, timeout);
        } else {
            // Nothing in the tail cache; look it up in the index.
            return this.recoveryTracker.waitIfNeeded(segment, () -> this.indexReader.getBackpointerOffset(segment, offset, timeout));
        }
    }

    /**
     * Performs a Batch Update or Removal.
     *
     * If {@link TableKeyBatch#isConditional()} returns true, this will execute an atomic Conditional Update/Removal based
     * on the condition items in the batch ({@link TableKeyBatch#getVersionedItems}. The entire TableKeyBatch will be
     * conditioned on those items, including those items that do not have a condition set. The entire TableKeyBatch will
     * either all be committed as one unit or not at all.
     *
     * Otherwise this will perform an Unconditional Update/Removal, where all the {@link TableKeyBatch.Item}s in
     * {@link TableKeyBatch#getItems()} will be applied regardless of whether they already exist or what their versions are.
     *
     * @param segment The Segment to perform the update/removal on.
     * @param batch   The {@link TableKeyBatch} to apply.
     * @param persist A Supplier that, when invoked, will persist the contents of the batch to the Segment and return
     *                a CompletableFuture to indicate when the operation is done, containing the offset at which the
     *                batch has been written to the Segment. This Future must complete successfully before the effects
     *                of the {@link TableKeyBatch} are applied to the in-memory Index or before downstream conditional
     *                updates on the affected keys are initiated.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain a list of offsets (within the Segment) where each
     * of the items in the batch has been persisted. If the update failed, it will be failed with the appropriate exception.
     * Notable exceptions:
     * <ul>
     * <li>{@link KeyNotExistsException} If a Key in the TableKeyBatch does not exist and was conditioned as having to exist.
     * <li>{@link BadKeyVersionException} If a Key does exist but had a version mismatch.
     * </ul>
     */
    CompletableFuture<List<Long>> update(DirectSegmentAccess segment, TableKeyBatch batch, Supplier<CompletableFuture<Long>> persist, TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        if (batch.isConditional()) {
            // Conditional update.
            // Collect all Cache Keys for the Update Items that have a condition on them; we need this on order to
            // serialize execution across them.
            val keys = batch.getVersionedItems().stream()
                            .map(item -> Maps.immutableEntry(segment.getSegmentId(), item.getHash()))
                            .collect(Collectors.toList());

            // Serialize the execution (queue it up to run only after all other currently queued up conditional updates
            // for touched keys have finished).
            return this.conditionalUpdateProcessor.add(
                    keys,
                    () -> validateConditionalUpdate(segment, batch, timer)
                            .thenComposeAsync(v -> persist.get(), this.executor)
                            .thenApplyAsync(batchOffset -> updateCache(segment, batch, batchOffset), this.executor));
        } else {
            // Unconditional update: persist the entries and update the cache.
            return persist.get().thenApplyAsync(batchOffset -> updateCache(segment, batch, batchOffset), this.executor);
        }
    }

    private List<Long> updateCache(DirectSegmentAccess segment, TableKeyBatch batch, long batchOffset) {
        // Ensure the cache knows about the Last Indexed Offset segment for this Segment. If it doesn't we need to fetch it.
        // This is necessary so we can properly record new backpointers into the cache which occur beyond the Last Indexed Offset
        // for this segment.
        this.cache.updateSegmentIndexOffsetIfMissing(segment.getSegmentId(), () -> this.indexReader.getLastIndexedOffset(segment.getInfo()));

        // Update the cache with the contents of the batch.
        return this.cache.includeUpdateBatch(segment.getSegmentId(), batch, batchOffset);
    }

    /**
     * Validates all the conditional updates specified in the given TableKeyBatch.
     *
     * @param segment The Segment to operate on.
     * @param batch   The TableKeyBatch to validate.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will indicate that validation succeeded. If the validation did
     * not pass, it will be failed with the appropriate exception. Notable exceptions:
     * <ul>
     * <li>{@link KeyNotExistsException} If a Key does not exist and was conditioned as having to exist.
     * <li>{@link BadKeyVersionException} If a Key does exist but had a version mismatch.
     * </ul>
     */
    private CompletableFuture<Void> validateConditionalUpdate(DirectSegmentAccess segment, TableKeyBatch batch, TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        List<UUID> hashes = batch.getVersionedItems().stream()
                .map(TableKeyBatch.Item::getHash)
                .collect(Collectors.toList());
        CompletableFuture<Void> result = getBucketOffsets(segment, hashes, timer)
                .thenAccept(offsets -> validateConditionalUpdate(batch.getVersionedItems(), offsets, segment.getInfo().getName()));
        return Futures.exceptionallyCompose(
                result,
                ex -> {
                    ex = Exceptions.unwrap(ex);
                    if (ex instanceof BadKeyVersionException) {
                        return validateConditionalUpdateFailures(segment, ((BadKeyVersionException) ex).getExpectedVersions(), timer);
                    }

                    // Some other exception. Re-throw.
                    return Futures.failedFuture(ex);
                });
    }

    /**
     * Validates a list of UpdateBatchItems against their actual Table Bucket offsets.
     *
     * @param items         A list of {@link TableKeyBatch.Item} instances to validate.
     * @param bucketOffsets A Map of Key Hashes to their corresponding offsets (versions). Each {@link TableKeyBatch.Item}
     *                      will be looked up in this Map (based on the Item's KeyHash) and validated appropriately.
     * @param segmentName   The name of the segment on which the update is performed.
     * @throws KeyNotExistsException  If an UpdateBatchItem's Key does not exist in the Table but the item's version does
     *                                not indicate that the key must not exist.
     * @throws BadKeyVersionException If an UpdateBatchItem's Key does exist in the Table but the item's version is
     *                                different from that key's version.
     */
    @SneakyThrows(ConditionalTableUpdateException.class)
    private void validateConditionalUpdate(List<TableKeyBatch.Item> items, Map<UUID, Long> bucketOffsets, String segmentName) {
        val badKeyVersions = new HashMap<TableKey, Long>(); // Key = Key that failed, Value = Key's bucket offset.
        for (val item : items) {
            // Validate compareVersion.
            TableKey key = item.getKey();
            Long bucketOffset = bucketOffsets.get(item.getHash());
            assert key.hasVersion() : "validateConditionalUpdate for TableKey with no compare version";
            if (bucketOffset == TableKey.NOT_EXISTS) {
                if (key.getVersion() != TableKey.NOT_EXISTS) {
                    // Key does not exist, but the conditional update provided a specific version.
                    throw new KeyNotExistsException(segmentName, key.getKey());
                }
            } else if (bucketOffset != key.getVersion()) {
                // Key does exist, but has the wrong version.
                badKeyVersions.put(key, bucketOffset);
            }
        }

        if (!badKeyVersions.isEmpty()) {
            // Throw the bad key version in bulk - helps speed verification.
            throw new BadKeyVersionException(segmentName, badKeyVersions);
        }

        // All validations for all items passed.
    }

    private CompletableFuture<Void> validateConditionalUpdateFailures(DirectSegmentAccess segment, Map<TableKey, Long> expectedVersions, TimeoutTimer timer) {
        assert !expectedVersions.isEmpty();
        val bucketReader = TableBucketReader.key(segment, this::getBackpointerOffset, this.executor);
        val searches = expectedVersions.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> bucketReader.find(e.getKey().getKey(), e.getValue(), timer)));
        return Futures
                .allOf(searches.values())
                .thenRun(() -> {
                    val failed = new HashMap<TableKey, Long>();
                    for (val e : searches.entrySet()) {
                        val actual = e.getValue().join();
                        boolean isValid = actual == null
                                ? e.getKey().getVersion() == TableKey.NOT_EXISTS
                                : e.getKey().getVersion() == actual.getVersion();

                        if (!isValid) {
                            failed.put(e.getKey(), actual == null ? TableKey.NOT_EXISTS : actual.getVersion());
                        }
                    }

                    if (!failed.isEmpty()) {
                        throw new CompletionException(new BadKeyVersionException(segment.getInfo().getName(), failed));
                    }
                });
    }

    /**
     * Notifies this ContainerKeyIndex instance that the {@link TableAttributes#INDEX_OFFSET} attribute value for the
     * given Segment has been changed.
     *
     * @param segmentId   The Id of the Segment whose Index Offset has changed.
     * @param indexOffset The new value for the Index Offset. A negative value indicates this segment has been evicted
     *                    from memory and relevant resources can be freed.
     */
    void notifyIndexOffsetChanged(long segmentId, long indexOffset) {
        this.cache.updateSegmentIndexOffset(segmentId, indexOffset);
        this.recoveryTracker.updateSegmentIndexOffset(segmentId, indexOffset);
    }

    /**
     * Gets the KeyHashes and their corresponding offsets for not-yet-indexed Table Buckets. These are updates
     * that have been accepted and written to the Segment but not yet indexed (persisted via the {@link IndexWriter}).
     *
     * @param segment A {@link DirectSegmentAccess} representing the Segment for which to get the Unindexed Key Hashes.
     * @return A CompletableFuture that, when completed, will contain the desired result. This Future will wait on any
     * Segment-specific recovery to complete before executing.
     */
    CompletableFuture<Map<UUID, CacheBucketOffset>> getUnindexedKeyHashes(DirectSegmentAccess segment) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.recoveryTracker.waitIfNeeded(segment,
                () -> CompletableFuture.completedFuture(this.cache.getTailHashes(segment.getSegmentId())));
    }

    //endregion

    //region RecoveryTracker

    /**
     * Helps keep track of Segment Recovery events.
     */
    @ThreadSafe
    private class RecoveryTracker implements AutoCloseable {
        @GuardedBy("this")
        private final HashSet<Long> recoveredSegments = new HashSet<>();
        @GuardedBy("this")
        private final HashMap<Long, RecoveryTask> recoveryTasks = new HashMap<>();

        @Override
        public void close() {
            List<RecoveryTask> toCancel;
            synchronized (this) {
                toCancel = new ArrayList<>(this.recoveryTasks.values());
                this.recoveryTasks.clear();
            }

            ObjectClosedException ex = new ObjectClosedException(ContainerKeyIndex.this);
            toCancel.forEach(t -> t.task.completeExceptionally(ex));
        }

        /**
         * Updates the SegmentIndexOffset for the given Segment and releases any blocked tasks, if appropriate.
         *
         * @param segmentId   The Segment id.
         * @param indexOffset The current Index Offset. -1 means it has been evicted and tasks should be cancelled.
         */
        void updateSegmentIndexOffset(long segmentId, long indexOffset) {
            boolean removed = indexOffset < 0;
            RecoveryTask task;
            synchronized (this) {
                if (removed) {
                    // Segment evicted. Free resources.
                    task = this.recoveryTasks.remove(segmentId);
                    this.recoveredSegments.remove(segmentId);
                } else {
                    task = this.recoveryTasks.get(segmentId);
                }

                if (task != null && !removed) {
                    if (indexOffset < task.triggerIndexOffset) {
                        // There is a task, but the trigger condition is not met.
                        task = null;
                    } else {
                        // Segment is fully recovered.
                        this.recoveryTasks.remove(segmentId);
                        this.recoveredSegments.add(segmentId);
                    }
                }
            }

            if (task != null) {
                if (removed) {
                    // Normally nobody should be waiting on this, but in case they did, there's nothing we can do about it now.
                    task.task.cancel(true);
                } else {
                    // Notify whoever is waiting that it's all clear to execute.
                    task.task.complete(null);
                }
            }
        }

        /**
         * Blocks the execution of the given task until the given Segment has completed a Table Index Recovery, if necessary.
         * If the Segment's Index is up-to-date, the given task is executed right away.
         *
         * @param segment   The Segment to execute the task on.
         * @param toExecute A Supplier that, when invoked, will execute a task and return a CompletableFuture which will
         *                  complete when the task is done.
         * @param <T>       Return type.
         * @return A CompletableFuture that will be completed when the task is done.
         */
        <T> CompletableFuture<T> waitIfNeeded(DirectSegmentAccess segment, Supplier<CompletableFuture<T>> toExecute) {
            RecoveryTask task = null;
            synchronized (this) {
                if (!this.recoveredSegments.contains(segment.getSegmentId())) {
                    // This segment wasn't marked as having completed recovery. Check its status.
                    task = this.recoveryTasks.get(segment.getSegmentId());
                    if (task == null) {
                        // Nobody waiting on it either.
                        SegmentProperties sp = segment.getInfo();
                        long segmentLength = sp.getLength();
                        long lastIndexedOffset = ContainerKeyIndex.this.indexReader.getLastIndexedOffset(sp);
                        if (lastIndexedOffset >= segmentLength) {
                            // Already caught up.
                            this.recoveredSegments.add(segment.getSegmentId());
                        } else {
                            // Need to catch up. Setup a RecoveryTask that will be completed once we are notified that
                            // the Segment's LastIndexedOffset is at least the current length.
                            task = new RecoveryTask(segmentLength);
                            this.recoveryTasks.put(segment.getSegmentId(), task);
                        }
                    }
                }
            }

            if (task == null) {
                // No recovery task. Execute right away.
                return toExecute.get();
            } else {
                // A recovery task is registered. Queue behind it.
                return task.task.thenComposeAsync(ignored -> toExecute.get(), executor);
            }
        }

        @RequiredArgsConstructor
        private class RecoveryTask {
            final long triggerIndexOffset;
            final CompletableFuture<Void> task = new CompletableFuture<>();
        }
    }

    //endregion
}