/**
 * Copyright Pravega Authors.
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
package io.pravega.segmentstore.server.tables;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.AsyncSemaphore;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.ConditionalTableUpdateException;
import io.pravega.segmentstore.contracts.tables.KeyNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentNotEmptyException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import java.io.IOException;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * A cache-backed, in-memory Table Index representation with pass-through updates. Enables Conditional Updates and caching
 * of recently used Keys for faster access.
 */
@ThreadSafe
@Slf4j
class ContainerKeyIndex implements AutoCloseable {
    //region Members
    @Getter
    private final IndexReader indexReader;
    private final ScheduledExecutorService executor;
    private final ContainerKeyCache cache;
    private final CacheManager cacheManager;
    private final MultiKeySequentialProcessor<Map.Entry<Long, UUID>> conditionalUpdateProcessor;
    private final ContainerSortedKeyIndex sortedKeyIndex;
    private final SegmentTracker segmentTracker;
    private final AtomicBoolean closed;
    private final KeyHasher keyHasher;
    private final String traceObjectId;
    private final int containerId;
    private final TableExtensionConfig config;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerKeyIndex class.
     *
     * @param containerId    Id of the SegmentContainer this instance is associated with.
     * @param config         Configuration.
     * @param cacheManager   A {@link CacheManager} that can be used to manage Cache instances.
     * @param sortedKeyIndex A {@link ContainerSortedKeyIndex} that can be used to manage {@link SegmentSortedKeyIndex}es.
     * @param keyHasher      A {@link KeyHasher} that can be used to hash keys.
     * @param executor       Executor for async operations.
     */
    ContainerKeyIndex(int containerId, @NonNull TableExtensionConfig config, @NonNull CacheManager cacheManager, @NonNull ContainerSortedKeyIndex sortedKeyIndex,
                      @NonNull KeyHasher keyHasher, @NonNull ScheduledExecutorService executor) {
        this.cache = new ContainerKeyCache(cacheManager.getCacheStorage());
        this.cacheManager = cacheManager;
        this.cacheManager.register(this.cache);
        this.executor = executor;
        this.indexReader = new IndexReader(executor);
        this.conditionalUpdateProcessor = new MultiKeySequentialProcessor<>(this.executor);
        this.sortedKeyIndex = sortedKeyIndex;
        this.segmentTracker = new SegmentTracker();
        this.keyHasher = keyHasher;
        this.closed = new AtomicBoolean();
        this.traceObjectId = String.format("KeyIndex[%d]", containerId);
        this.containerId = containerId;
        this.config = config;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.conditionalUpdateProcessor.close();
            this.cacheManager.unregister(this.cache);
            this.cache.close();
            this.segmentTracker.close();
            log.info("{}: Closed.", this.traceObjectId);
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
     * @param timer   Timer for the operation.
     * @param <T>     Return type.
     * @return A CompletableFuture that, when completed normally, will indicate that the action was executed. If it failed,
     * or if the segment is not empty, it will be failed with the appropriate exception. Notable exceptions:
     * <ul>
     * <li>{@link TableSegmentNotEmptyException} If the Segment is not empty.
     * </ul>
     */
    <T> CompletableFuture<T> executeIfEmpty(DirectSegmentAccess segment, Supplier<CompletableFuture<T>> action, TimeoutTimer timer) {
        return this.segmentTracker.waitIfNeeded(segment, ignored -> this.conditionalUpdateProcessor.addWithFilter(
                conditionKey -> conditionKey.getKey() == segment.getSegmentId(),
                () -> isTableSegmentEmpty(segment, timer)
                        .thenCompose(isEmpty -> {
                            if (isEmpty) {
                                // Segment is empty.
                                return action.get();
                            } else {
                                // Segment is not empty.
                                return Futures.failedFuture(new TableSegmentNotEmptyException(segment.getInfo().getName()));
                            }
                        })));
    }

    /**
     * Determines if a Table Segment is empty or not, by accounting for both the indexed portion and the unindexed (tail)
     * section.
     *
     * @param segment The Segment to verify.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will indicate if the Table Segment is empty or not.
     */
    private CompletableFuture<Boolean> isTableSegmentEmpty(DirectSegmentAccess segment, TimeoutTimer timer) {
        // Get a snapshot of the Tail Index and identify all bucket removals.
        val tailHashes = this.cache.getTailHashes(segment.getSegmentId());
        val tailRemovals = tailHashes.entrySet().stream()
                                     .filter(e -> e.getValue().isRemoval())
                                     .map(Map.Entry::getKey)
                                     .collect(Collectors.toList());
        if (tailHashes.size() > tailRemovals.size()) {
            // Tail Index has at least one update, which implies the Table Segment is not empty.
            return CompletableFuture.completedFuture(false);
        } else {
            // Get the number of indexed Table Buckets.
            SegmentProperties sp = segment.getInfo();
            long indexedBucketCount = this.indexReader.getBucketCount(sp);
            if (tailRemovals.isEmpty()) {
                // No removals in the Tail index, so we can derive our response from the total number of indexed buckets.
                return CompletableFuture.completedFuture(indexedBucketCount <= 0);
            } else {
                // Tail Index has at least one removal. We need to check which of these removals point to a real Table Bucket.
                // It is possible that we received an unconditional remove for a Table Bucket that does not exist or a Table
                // Bucket has been created and immediately deleted (before being included in the main Index). In order to
                // determine if the table is empty, we need to figure out the exact count of removed buckets.
                return this.indexReader.locateBuckets(segment, tailRemovals, timer)
                                       .thenApply(buckets -> {
                                           long removedCount = buckets.values().stream().filter(TableBucket::exists).count();
                                           return indexedBucketCount <= removedCount;
                                       });
            }
        }
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
            return this.segmentTracker.waitIfNeeded(segment, cacheUpdated -> getBucketOffsetFromSegment(segment, result, toLookup, cacheUpdated, timer));
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
     */
    CompletableFuture<Long> getBucketOffsetDirect(DirectSegmentAccess segment, UUID keyHash, TimeoutTimer timer) {
        // Get the bucket offset from the segment, which will update the cache if actually newer.
        return this.segmentTracker.waitIfNeeded(segment,
                cacheUpdated -> getBucketOffsetFromSegment(segment, Collections.synchronizedMap(new HashMap<>()), Collections.singleton(keyHash), cacheUpdated, timer)
                        .thenApply(result -> result.get(keyHash)));
    }

    private CompletableFuture<Map<UUID, Long>> getBucketOffsetFromSegment(DirectSegmentAccess segment, Map<UUID, Long> result,
                                                                          Collection<UUID> toLookup, boolean tryCache, TimeoutTimer timer) {
        return this.indexReader
                .locateBuckets(segment, toLookup, timer)
                .thenApplyAsync(bucketsByHash -> {
                    for (val e : bucketsByHash.entrySet()) {
                        UUID keyHash = e.getKey();
                        TableBucket bucket = e.getValue();

                        // Cache the bucket's location, but only if it exists.
                        if (bucket.exists()) {
                            // Update the cache information.
                            long highestOffset = this.cache.includeExistingKey(
                                    segment.getSegmentId(), keyHash, bucket.getSegmentOffset());
                            result.put(keyHash, highestOffset);
                        } else if (tryCache) {
                            // We were instructed to retry the cache.
                            val existingValue = this.cache.get(segment.getSegmentId(), keyHash);
                            result.put(keyHash, existingValue == null || existingValue.isRemoval() ? TableKey.NOT_EXISTS : existingValue.getSegmentOffset());
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
            return this.segmentTracker.waitIfNeeded(segment, ignored -> this.indexReader.getBackpointerOffset(segment, offset, timeout));
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

        Supplier<CompletableFuture<List<Long>>> update;
        if (batch.isConditional()) {
            // Conditional update.
            // Collect all Cache Keys for the Update Items that have a condition on them; we need this on order to
            // serialize execution across them.
            val keys = batch.getVersionedItems().stream()
                            .map(item -> Maps.immutableEntry(segment.getSegmentId(), item.getHash()))
                            .collect(Collectors.toList());

            // Serialize the execution (queue it up to run only after all other currently queued up conditional updates
            // for touched keys have finished).
            update = () -> this.conditionalUpdateProcessor.add(
                    keys,
                    () -> validateConditionalUpdate(segment, batch, timer)
                            .thenComposeAsync(v -> persist.get(), this.executor)
                            .thenApplyAsync(batchOffset -> updateCache(segment, batch, batchOffset), this.executor));
        } else {
            // Unconditional update: persist the entries and update the cache.
            update = () -> persist.get().thenApplyAsync(batchOffset -> updateCache(segment, batch, batchOffset), this.executor);
        }

        // Throttle any requests, if needed. Note that Internal requests (which are made by the WriterTableProcessor)
        // must not be throttled, otherwise we risk blocking ourselves and never release the throttle.
        return this.segmentTracker.throttleIfNeeded(segment, update, batch.getLength(), !batch.isExternal());
    }

    private List<Long> updateCache(DirectSegmentAccess segment, TableKeyBatch batch, long batchOffset) {
        this.sortedKeyIndex.getSortedKeyIndex(segment.getSegmentId(), segment.getInfo()).includeTailUpdate(batch, batchOffset);

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
                .collect(Collectors.toMap(Map.Entry::getKey, e -> findBucketEntry(segment, bucketReader, e.getKey().getKey(), e.getValue(), timer)));
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
     * Finds the actual version of a Table Key while accounting for TableBucket rearrangements/relocations due to
     * concurrent compactions.
     * <p>
     * This is handled by detecting a {@link StreamSegmentTruncatedException} and bypassing the cache in that case to
     * read the TableBucket offset directly from the index, which should already contain the updated value (indexing is
     * done prior to truncation).
     *
     * @param segment      A {@link DirectSegmentAccess} to read from.
     * @param bucketReader A {@link TableBucketReader} to use to parse the table bucket.
     * @param key          The sought key.
     * @param bucketOffset The {@link TableBucket} offset to begin from.
     * @param timer        Timer
     * @param <T>          Return type.
     */
    <T> CompletableFuture<T> findBucketEntry(DirectSegmentAccess segment, TableBucketReader<T> bucketReader,
                                             BufferView key, long bucketOffset, TimeoutTimer timer) {
        // We first attempt an optimistic read, which involves fewer steps, and only invalidate the cache and read
        // directly from the index if unable to find anything and there is a chance the sought key actually exists.
        // Encountering a truncated Segment offset indicates that the Segment may have recently been compacted and
        // we are using a stale cache value.
        return Futures.exceptionallyExpecting(bucketReader.find(key, bucketOffset, timer),
                ex -> ex instanceof StreamSegmentTruncatedException, null)
                .thenComposeAsync(entry -> {
                    if (entry != null) {
                        // We found an entry.
                        return CompletableFuture.completedFuture(entry);
                    } else {
                        // We have a valid TableBucket but were unable to locate the key using the cache, either
                        // because the cache points to a truncated offset or because we are unable to determine
                        // if the TableBucket has been rearranged due to a compaction. The rearrangement is a rare
                        // occurrence and can only happen if more than one Key is mapped to a bucket (collision).
                        return getBucketOffsetDirect(segment, this.keyHasher.hash(key), timer)
                                .thenComposeAsync(newOffset -> bucketReader.find(key, newOffset, timer), this.executor);
                    }
                }, this.executor);
    }

    /**
     * Notifies this ContainerKeyIndex instance that the {@link TableAttributes#INDEX_OFFSET} attribute value for the
     * given Segment has been changed.
     *
     * @param segmentId      The Id of the Segment whose Index Offset has changed.
     * @param indexOffset    The new value for the Index Offset. A negative value indicates this segment has been evicted
     *                       from memory and relevant resources can be freed.
     * @param processedBytes The total number of bytes processed during this update (including duplicates).
     */
    void notifyIndexOffsetChanged(long segmentId, long indexOffset, int processedBytes) {
        this.cache.updateSegmentIndexOffset(segmentId, indexOffset);
        this.sortedKeyIndex.notifyIndexOffsetChanged(segmentId, indexOffset);
        this.segmentTracker.updateSegmentIndexOffset(segmentId, indexOffset, processedBytes);
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
        return this.segmentTracker.waitIfNeeded(segment,
                ignored -> CompletableFuture.completedFuture(this.cache.getTailHashes(segment.getSegmentId())));
    }

    /**
     * Gets the {@link SegmentSortedKeyIndex} associated with the given Segment, as provided by
     * {@link ContainerSortedKeyIndex#getSortedKeyIndex}. This can be used to safely iterate through Keys of a Sorted
     * Table Segment while including both fully indexed and tail updates.
     *
     * Note: this will return the same result as {@link ContainerSortedKeyIndex#getSortedKeyIndex}, however this method
     * will wait on any Segment-specific recovery (and trigger it) to complete before executing, which should enable a
     * safe iteration for recently recovered Table Segments.
     *
     * @param segment A {@link DirectSegmentAccess} representing the Segment for which to get the {@link SegmentSortedKeyIndex}.
     * @return A CompletableFuture that, when completed, will contain the desired result. This Future will wait on any
     * Segment-specific recovery to complete before executing.
     */
    CompletableFuture<SegmentSortedKeyIndex> getSortedKeyIndex(DirectSegmentAccess segment) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.segmentTracker.waitIfNeeded(segment,
                ignored -> CompletableFuture.completedFuture(this.sortedKeyIndex.getSortedKeyIndex(segment.getSegmentId(), segment.getInfo())));
    }

    /**
     * Reads the tail section (beyond {@link TableAttributes#INDEX_OFFSET}) of the given segment and caches the latest
     * values recorded there in the tail index.
     *
     * The operation will not execute if any of the following is true:
     * - The tail section has a length of 0.
     * - The tail section has a length exceeding {@link TableExtensionConfig#getMaxTailCachePreIndexLength()}.
     *
     * This method triggers this operation asynchronously and does not wait for it to complete. Its completion status and
     * any errors will be logged.
     *
     * NOTE: this does not peform a proper recovery nor does it update the durable index - it simply caches the values.
     * The {@link WriterTableProcessor} must be used to update the index.
     *
     * @param segment A {@link DirectSegmentAccess} representing the Segment for which to cache the tail index.
     */
    private void triggerCacheTailIndex(DirectSegmentAccess segment, long lastIndexedOffset, long segmentLength) {
        long tailIndexLength = segmentLength - lastIndexedOffset;
        if (lastIndexedOffset >= segmentLength) {
            // Fully caught up. Nothing else to do.
            log.debug("{}: Table Segment {} fully indexed.", this.traceObjectId, segment.getSegmentId());
            return;
        } else if (tailIndexLength > this.config.getMaxTailCachePreIndexLength()) {
            log.debug("{}: Table Segment {} cannot perform tail-caching because tail index too long ({}).", this.traceObjectId,
                    segment.getSegmentId(), tailIndexLength);
            return;
        }

        // Read the tail section of the segment and process its updates. All of this should already be in the cache so
        // we are not going to do any Storage reads.
        SegmentProperties segmentInfo = segment.getInfo();
        boolean sorted = ContainerSortedKeyIndex.isSortedTableSegment(segmentInfo);
        log.debug("{}: Tail-caching started for Table Segment {}. Sorted={}, LastIndexedOffset={}, SegmentLength={}.",
                this.traceObjectId, segment.getSegmentId(), sorted, lastIndexedOffset, segmentLength);
        ReadResult rr = segment.read(lastIndexedOffset, (int) tailIndexLength, this.config.getRecoveryTimeout());
        AsyncReadResultProcessor
                .processAll(rr, this.executor, this.config.getRecoveryTimeout())
                .thenAcceptAsync(inputData -> {
                    // Parse out all Table Keys and collect their latest offsets, as well as whether they were deleted.
                    val updates = new TailUpdates(sorted);
                    collectLatestOffsets(inputData, lastIndexedOffset, (int) tailIndexLength, updates);

                    // Incorporate that into the cache.
                    this.cache.includeTailCache(segment.getSegmentId(), updates.byBucket);

                    // Incorporate it into the Sorted Key Index.
                    this.sortedKeyIndex.getSortedKeyIndex(segment.getSegmentId(), segmentInfo).includeTailCache(updates.byKey);

                    log.debug("{}: Tail-caching complete for Table Segment {}. Key Update Count={}, Bucket Update Count={}.",
                            this.traceObjectId, segment.getSegmentId(), updates.byKey.size(), updates.byBucket.size());

                    // Notify the Segment Tracker that this segment has been recovered, so it can unblock any calls it
                    // may have collected.
                    this.segmentTracker.updateSegmentIndexOffset(segment.getSegmentId(), segmentLength, 0, updates.byBucket.size() > 0);
                }, this.executor)
                .exceptionally(ex -> {
                    log.warn("{}: Tail-caching failed for Table Segment {}.", this.traceObjectId, segment.getSegmentId(), Exceptions.unwrap(ex));
                    return null;
                });
    }

    @SneakyThrows(IOException.class)
    private void collectLatestOffsets(BufferView input, long startOffset, int maxLength, TailUpdates result) {
        EntrySerializer serializer = new EntrySerializer();
        long nextOffset = startOffset;
        final long maxOffset = startOffset + maxLength;
        val inputReader = input.getBufferViewReader();
        while (nextOffset < maxOffset) {
            val e = AsyncTableEntryReader.readEntryComponents(inputReader, nextOffset, serializer);
            val hash = this.keyHasher.hash(e.getKey());
            result.add(e.getKey(), hash, nextOffset, e.getHeader().isDeletion());
            nextOffset += e.getHeader().getTotalLength();
        }
    }

    @VisibleForTesting
    long getUnindexedSizeBytes(long segmentId) {
        return this.segmentTracker.getUnindexedSizeBytes(segmentId);
    }

    //endregion

    //region TailUpdates

    /**
     * Aggregates non-indexed updates for quick tail-indexing.
     */
    @RequiredArgsConstructor
    private static class TailUpdates {
        final Map<UUID, CacheBucketOffset> byBucket = new HashMap<>();
        final Map<BufferView, CacheBucketOffset> byKey = new HashMap<>();
        private final boolean recordByKey;

        void add(BufferView key, UUID keyHash, long offset, boolean isDeletion) {
            CacheBucketOffset cbo = new CacheBucketOffset(offset, isDeletion);
            this.byBucket.put(keyHash, cbo);
            if (this.recordByKey) {
                this.byKey.put(key, cbo);
            }
        }
    }

    //endregion

    //region SegmentTracker

    /**
     * Helps keep track of Segment-specific state.
     */
    @ThreadSafe
    private class SegmentTracker implements AutoCloseable {
        @GuardedBy("this")
        private final HashSet<Long> recoveredSegments = new HashSet<>();
        @GuardedBy("this")
        private final HashMap<Long, RecoveryTask> recoveryTasks = new HashMap<>();
        @GuardedBy("this")
        private final HashMap<Long, AsyncSemaphore> throttlers = new HashMap<>();

        @Override
        public void close() {
            List<RecoveryTask> toCancel;
            List<AsyncSemaphore> toClose;
            synchronized (this) {
                toCancel = new ArrayList<>(this.recoveryTasks.values());
                this.recoveryTasks.clear();
                toClose = new ArrayList<>(this.throttlers.values());
                this.throttlers.clear();
            }

            ObjectClosedException ex = new ObjectClosedException(ContainerKeyIndex.this);
            toCancel.forEach(task -> {
                task.task.completeExceptionally(ex);
                log.info("{}: Cancelled one or more tasks that were waiting on Table Segment {} recovery.", traceObjectId, task.segmentId);
            });
            toClose.forEach(AsyncSemaphore::close);
        }

        /**
         * Updates the SegmentIndexOffset for the given Segment and releases any blocked tasks, if appropriate.
         *
         * @param segmentId          The Segment id.
         * @param indexOffset        The current Index Offset. -1 means it has been evicted and tasks should be cancelled.
         * @param processedSizeBytes The total size, in bytes, of the processed entries.
         */
        void updateSegmentIndexOffset(long segmentId, long indexOffset, int processedSizeBytes) {
            updateSegmentIndexOffset(segmentId, indexOffset, processedSizeBytes, false);
        }

        /**
         * Updates the SegmentIndexOffset for the given Segment and releases any blocked tasks, if appropriate.
         *
         * @param segmentId          The Segment id.
         * @param indexOffset        The current Index Offset. -1 means it has been evicted and tasks should be cancelled.
         * @param processedSizeBytes The total size, in bytes, of the processed entries.
         * @param cacheUpdated       True if the cache was updated during the recovery process.
         */
        void updateSegmentIndexOffset(long segmentId, long indexOffset, int processedSizeBytes, boolean cacheUpdated) {
            boolean removed = indexOffset < 0;
            RecoveryTask task;
            AsyncSemaphore throttler;
            synchronized (this) {
                task = this.recoveryTasks.get(segmentId);
                throttler = this.throttlers.get(segmentId);
                if (removed) {
                    // Segment evicted. Free resources.
                    this.recoveredSegments.remove(segmentId);
                    this.throttlers.remove(segmentId);
                }

                if (task != null && !removed) {
                    if (indexOffset < task.triggerIndexOffset) {
                        // There is a task, but the trigger condition is not met.
                        log.debug("{}: For TableSegment {}, IndexOffset={}, TriggerOffset={}.", traceObjectId, segmentId, indexOffset, task.triggerIndexOffset);
                        task = null;
                    } else {
                        // Segment is fully recovered.
                        this.recoveredSegments.add(segmentId);
                    }
                }
            }

            if (task != null) {
                if (removed) {
                    // Normally nobody should be waiting on this, but in case they did, there's nothing we can do about it now.
                    log.debug("{}: TableSegment {} evicted; cancelling dependent tasks.", traceObjectId, segmentId);
                    task.task.cancel(true);
                } else {
                    // Notify whoever is waiting that it's all clear to execute.
                    log.debug("{}: TableSegment {} fully recovered; triggering dependent tasks.", traceObjectId, segmentId);
                    task.task.complete(cacheUpdated);
                }
            }

            if (throttler != null) {
                if (removed) {
                    // We evicted the segment, so close the throttler.
                    throttler.close();
                } else if (processedSizeBytes >= 0) {
                    // We have made indexing progress. Update the throttler with the size.
                    throttler.release(processedSizeBytes);
                }
            }
        }

        /**
         * Determines if the given task can be executed immediately or must be delayed until more items are indexed.
         * Each Segment can have a maximum number of unindexed bytes (defined by {@link TableExtensionConfig#getMaxUnindexedLength});
         * if this limit is reached then the {@link IndexWriter} has fallen too far behind and we need to slow down the
         * ingestion of new updates (otherwise we risk lengthy delays upon a subsequent recovery).
         *
         * @param segment    The Segment to execute the task on.
         * @param toExecute  A Supplier that, when invoked, will execute a task and return a CompletableFuture which will
         *                   complete when the task is done.
         * @param updateSize The number of bytes that this task requires.
         * @param force      If true, will set the {@code force} flag on the underlying throttler, which will cause this
         *                   task to execute right away even if there is no capacity for it (its credits will still be recorded).
         * @param <T>        Return type.
         * @return A CompletableFuture that will be completed when the task is done. If executing immediately, this is the
         * result of toExecute, otherwise it will be a different Future which will be completed when toExecute completes.
         */
        <T> CompletableFuture<T> throttleIfNeeded(DirectSegmentAccess segment, Supplier<CompletableFuture<T>> toExecute, int updateSize, boolean force) {
            AsyncSemaphore throttler;
            synchronized (this) {
                throttler = this.throttlers.getOrDefault(segment.getSegmentId(), null);
                if (throttler == null) {
                    val si = segment.getInfo();
                    long initialDelta = Math.max(0, si.getLength() - ContainerKeyIndex.this.indexReader.getLastIndexedOffset(si));
                    throttler = new AsyncSemaphore(config.getMaxUnindexedLength(), initialDelta, String.format("%s-%s", containerId, segment.getSegmentId()));
                    this.throttlers.put(segment.getSegmentId(), throttler);
                }
            }

            return throttler.run(toExecute, updateSize, force);
        }

        /**
         * Gets the (current) number of unindexed bytes for the Segment with given Segment Id.
         *
         * @param segmentId The Id of the Segment to query.
         * @return The number of unindexed bytes, or 0 if indexing is up-to-date or the Segment is not registered.
         */
        long getUnindexedSizeBytes(long segmentId) {
            synchronized (this) {
                val throttler = this.throttlers.getOrDefault(segmentId, null);
                return throttler == null ? 0 : throttler.getUsedCredits();
            }
        }

        /**
         * Blocks the execution of the given task until the given Segment has completed a Table Index Recovery, if necessary.
         * If the Segment's Index is up-to-date, the given task is executed right away.
         *
         * @param segment   The Segment to execute the task on.
         * @param toExecute A Function that, when invoked, will execute a task and return a CompletableFuture which will
         *                  complete when the task is done. The argument to this Function is a boolean indicating whether
         *                  the Cache has been updated during the recovery.
         * @param <T>       Return type.
         * @return A CompletableFuture that will be completed when the task is done.
         */
        <T> CompletableFuture<T> waitIfNeeded(DirectSegmentAccess segment, Function<Boolean, CompletableFuture<T>> toExecute) {
            RecoveryTask task = null;
            long segmentLength = -1;
            long lastIndexedOffset = -1;
            boolean firstTask = false;
            synchronized (this) {
                if (!this.recoveredSegments.contains(segment.getSegmentId())) {
                    // This segment wasn't marked as having completed recovery. Check its status.
                    task = this.recoveryTasks.get(segment.getSegmentId());
                    if (task == null) {
                        // Nobody waiting on it either.
                        SegmentProperties sp = segment.getInfo();
                        segmentLength = sp.getLength();
                        lastIndexedOffset = ContainerKeyIndex.this.indexReader.getLastIndexedOffset(sp);
                        if (lastIndexedOffset >= segmentLength) {
                            // Already caught up.
                            this.recoveredSegments.add(segment.getSegmentId());
                        } else {
                            // Need to catch up. Setup a RecoveryTask that will be completed once we are notified that
                            // the Segment's LastIndexedOffset is at least the current length.
                            task = new RecoveryTask(segment.getSegmentId(), segmentLength);
                            this.recoveryTasks.put(segment.getSegmentId(), task);
                            firstTask = true;
                        }
                    }
                }
            }

            if (task == null) {
                // No recovery task. Execute right away.
                return toExecute.apply(false);
            } else {
                log.debug("{}: TableSegment {} is not fully recovered. Queuing 1 task.", traceObjectId, segment.getSegmentId());
                if (firstTask) {
                    setupRecoveryTask(task);
                    assert lastIndexedOffset >= 0;
                    triggerCacheTailIndex(segment, lastIndexedOffset, segmentLength);
                }

                // A recovery task is registered. Queue behind it.
                return task.task.thenComposeAsync(toExecute, executor);
            }
        }

        private void setupRecoveryTask(RecoveryTask task) {
            // Cancel the recovery task (and future that's waiting on it) if it took too long. This will prevent anyone
            // waiting on this from getting stuck forever. In case of an external retry, the state of the Table Segment
            // will be reinspected and the associated request may go through (if the recovery completed) or be blocked
            // again (and trigger another pre-cache).
            ScheduledFuture<Boolean> sf = executor.schedule(
                    () -> task.task.completeExceptionally(new TimeoutException(String.format("Table Segment %d recovery timed out.", task.segmentId))),
                    config.getRecoveryTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);

            // Cleanup whenever the task is done.
            task.task.whenComplete((r, ex) -> {
                synchronized (this) {
                    // Cleanup.
                    RecoveryTask removed = this.recoveryTasks.remove(task.segmentId);
                    if (removed != task) {
                        // Some weird thing happened and we removed a different task. Add it back.
                        this.recoveryTasks.put(task.segmentId, removed);
                    }
                }

                // It doesn't matter much if we attempt to cancel this ScheduledFuture before or after unregistering it.
                // If it has already been completed (normally or exceptionally), this will have no effect. It is preferred
                // to cancel it after being unregistered since that helps unit testing.
                sf.cancel(true);
            });
        }

        @RequiredArgsConstructor
        private class RecoveryTask {
            final long segmentId;
            final long triggerIndexOffset;
            final CompletableFuture<Boolean> task = new CompletableFuture<>();
        }
    }

    //endregion
}