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

import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.ConcurrentDependentProcessor;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.ConditionalTableUpdateException;
import io.pravega.segmentstore.contracts.tables.KeyNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.storage.CacheFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.NonNull;
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
    private final ConcurrentDependentProcessor<ContainerKeyCache.CacheKey, List<Long>> conditionalUpdateProcessor;
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
        this.conditionalUpdateProcessor = new ConcurrentDependentProcessor<>(this.executor);
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.conditionalUpdateProcessor.close();
            this.cacheManager.unregister(this.cache);
        }
    }

    //endregion

    //region Operations

    /**
     * Find the Last Bucket Offsets for the given {@link KeyHash}es.
     *
     * @param segment Segment to look up Bucket Offsets for.
     * @param hashes  A list of {@link KeyHash} to identify the Buckets.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the sought Offsets, in the same order as the given
     * hashes. If a particular bucket does not exist, {@link TableKey#NOT_EXISTS} will be inserted in its place.
     */
    CompletableFuture<List<Long>> getBucketOffsets(DirectSegmentAccess segment, List<KeyHash> hashes, TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        if (hashes.isEmpty()) {
            // Nothing to search.
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        // Find those keys which already exist in the cache.
        ArrayList<Long> result = new ArrayList<>();
        HashMap<KeyHash, Integer> toLookup = new HashMap<>();
        for (int i = 0; i < hashes.size(); i++) {
            KeyHash hash = hashes.get(i);
            val existingValue = this.cache.get(segment.getSegmentId(), hash);
            if (existingValue == null) {
                // Key does not exist in the cache (it may or may not exist at all). Add a placeholder and keep track of
                // it so we can look it up.
                result.add(TableKey.NOT_EXISTS);
                toLookup.put(hash, i);
            } else if (existingValue.isPresent()) {
                // Key exists.
                result.add(existingValue.getSegmentOffset());
            } else {
                // Key does not exist at all (deleted or really not exists). No need to do any other lookups.
                result.add(TableKey.NOT_EXISTS);
            }
        }

        if (toLookup.isEmpty()) {
            // Full cache hit.
            return CompletableFuture.completedFuture(result);
        } else {
            // Fetch information for missing hashes. TODO wait on recovery if needed.
            return this.indexReader.locateBuckets(toLookup.keySet(), segment, timer)
                    .thenApplyAsync(bucketsByHash -> {
                        for (val e : bucketsByHash.entrySet()) {
                            KeyHash keyHash = e.getKey();
                            TableBucket bucket = e.getValue();
                            Integer resultOffset = toLookup.get(keyHash);
                            assert resultOffset != null : "Unable to locate resultOffset based on KeyHash";

                            // Cache the bucket's location, but only if its path is complete.
                            if (bucket.isPartial()) {
                                // Incomplete bucket. What we are looking for does not exist. Do not update the information
                                // in the cache as this would have the potential to fill up the cache with useless keys
                                // if the application requests a lot of them (excellent DoS opportunity!).
                                result.set(resultOffset, TableKey.NOT_EXISTS);
                            } else {
                                // Update the cache information.
                                long highestOffset = this.cache.updateKey(segment.getSegmentId(), keyHash, this.indexReader.getOffset(bucket.getLastNode()));
                                result.set(resultOffset, highestOffset);
                            }
                        }

                        return result;
                    }, this.executor);
        }
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
        // TODO: figure out a way to temporarily perserve backpointers until the writer processor persists them into the index.
        return this.indexReader.getBackpointerOffset(offset, segment, timeout);
    }

    /**
     * Performs a Batch Update or Removal.
     *
     * If {@link TableKeyBatch#isConditional()} returns true, this will execute an atomic Conditional Update/Removal based
     * on the condition items in the batch ({@link TableKeyBatch#versionedItems}. The entire TableKeyBatch will be
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
            // Conditional removal.
            // Collect all Cache Keys for the Update Items that have a condition on them; we need this on order to
            // serialize execution across them.
            val keys = batch.getVersionedItems().stream()
                    .map(item -> new ContainerKeyCache.CacheKey(segment.getSegmentId(), item.getHash()))
                    .collect(Collectors.toList());

            // Serialize the execution (queue it up to run only after all other currently queued up conditional updates
            // for touched keys have finished).
            return this.conditionalUpdateProcessor.add(
                    keys,
                    () -> validateConditionalUpdate(segment, batch, timer)
                            .thenComposeAsync(v -> persist.get(), this.executor)
                            .thenApplyAsync(batchOffset -> this.cache.updateBatch(segment.getSegmentId(), batch, batchOffset), this.executor));
        } else {
            // Unconditional removal: persist the entries and update the cache.
            return persist.get().thenApply(batchOffset -> this.cache.updateBatch(segment.getSegmentId(), batch, batchOffset));
        }
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
        List<KeyHash> hashes = batch.getVersionedItems().stream()
                .map(TableKeyBatch.Item::getHash)
                .collect(Collectors.toList());
        return getBucketOffsets(segment, hashes, timer)
                .thenAccept(offsets -> validateConditionalUpdate(batch.getVersionedItems(), offsets, segment.getInfo().getName()));
    }

    /**
     * Validates a list of UpdateBatchItems against their actual Table Bucket offsets.
     *
     * @param items         A list of UpdateBatchItems to validate.
     * @param bucketOffsets A list of Offsets representing the current Offsets the given UpdateBatchItems. These offsets
     *                      must be in the same order as the given UpdateBatchItems.
     * @param segmentName   The name of the segment on which the update is performed.
     * @throws KeyNotExistsException  If an UpdateBatchItem's Key does not exist in the Table but the item's version does
     *                                not indicate that the key must not exist.
     * @throws BadKeyVersionException If an UpdateBatchItem's Key does exist in the Table but the item's version is
     *                                different from that key's version.
     */
    @SneakyThrows(ConditionalTableUpdateException.class)
    private void validateConditionalUpdate(List<TableKeyBatch.Item> items, List<Long> bucketOffsets, String segmentName) {
        assert items.size() == bucketOffsets.size() : "items.size() != bucketOffsets.size()";

        for (int i = 0; i < items.size(); i++) {
            // Validate compareVersion.
            TableKey key = items.get(i).getKey();
            Long bucketOffset = bucketOffsets.get(i);
            assert key.hasVersion() : "validateConditionalUpdate for TableKey with no compare version";
            if (bucketOffset == TableKey.NOT_EXISTS) {
                if (key.getVersion() != TableKey.NOT_EXISTS) {
                    // Key does not exist, but the conditional update provided a specific version.
                    throw new KeyNotExistsException(segmentName, key.getKey());
                }
            } else if (bucketOffset != key.getVersion()) {
                // Key does exist, but has the wrong version.
                throw new BadKeyVersionException(segmentName, key.getKey(), bucketOffset, key.getVersion());
            }
        }

        // All validations for all items passed.
    }

    //endregion
}