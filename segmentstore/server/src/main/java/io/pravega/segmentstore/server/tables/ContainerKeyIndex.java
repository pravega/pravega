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
import io.pravega.common.util.BitConverter;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.ConditionalTableUpdateException;
import io.pravega.segmentstore.contracts.tables.KeyNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.CacheFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

@ThreadSafe
class ContainerKeyIndex implements CacheManager.Client, AutoCloseable {
    //region Members
    @Getter
    private final IndexReader indexReader;
    private final ScheduledExecutorService executor;
    private final Cache cache;
    @GuardedBy("cacheEntries")
    private final Map<CacheKey, CacheEntry> cacheEntries;
    @GuardedBy("cacheEntries")
    private final Map<Long, Long> segmentIndexOffsets; // TODO: this needs updating once we wire up Writer Processors.
    @GuardedBy("cacheEntries")
    private int currentCacheGeneration;
    private final ConcurrentDependentProcessor<CacheKey, List<Long>> conditionalUpdateProcessor;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    ContainerKeyIndex(int containerId, @NonNull CacheFactory cacheFactory, @NonNull ScheduledExecutorService executor) {
        this.cache = cacheFactory.getCache(String.format("Container_%d_TableKeys", containerId));
        this.executor = executor;
        this.indexReader = new IndexReader(executor);
        this.conditionalUpdateProcessor = new ConcurrentDependentProcessor<>(this.executor);
        this.cacheEntries = new HashMap<>();
        this.segmentIndexOffsets = new HashMap<>();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.conditionalUpdateProcessor.close();
            this.cache.close();
            synchronized (this.cacheEntries) {
                this.cacheEntries.clear();
            }
        }
    }

    //endregion

    //region CacheManager.Client Implementation

    @Override
    public CacheManager.CacheStatus getCacheStatus() {
        int minGen = 0;
        int maxGen = 0;
        long size = 0;
        synchronized (this.cacheEntries) {
            for (CacheEntry e : this.cacheEntries.values()) {
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
        ArrayList<CacheKey> toRemove = new ArrayList<>();
        HashSet<Long> remainingSegmentIds = new HashSet<>();
        synchronized (this.cacheEntries) {
            this.currentCacheGeneration = currentGeneration;
            for (val e : this.cacheEntries.entrySet()) {
                CacheEntry entry = e.getValue();
                long indexedOffset = this.segmentIndexOffsets.getOrDefault(e.getKey().segmentId, 0L);
                if (entry.getGeneration() < oldestGeneration && entry.getHighestOffset() < indexedOffset) {
                    toRemove.add(e.getKey());
                } else {
                    remainingSegmentIds.add(e.getKey().segmentId);
                }
            }

            // Clear the expired cache entries.
            toRemove.forEach(this.cacheEntries::remove);

            // Remove those segment offset caches that are no longer used.
            this.segmentIndexOffsets.keySet().removeIf(segmentId -> !remainingSegmentIds.contains(segmentId));
        }

        // Remove from the Cache. It's ok to do this outside of the lock as the cache is thread safe.
        toRemove.forEach(this.cache::remove);
        return sizeRemoved;
    }

    //endregion

    //region Operations

    /**
     * Find the Last Bucket Offsets for the given {@link KeyHash}es.
     * TODO: maybe return lengths too?
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
        HashMap<KeyHash, LookupInfo> toLookup = new HashMap<>();
        for (int i = 0; i < hashes.size(); i++) {
            KeyHash hash = hashes.get(i);
            CacheKey cacheKey = new CacheKey(segment.getSegmentId(), hash.hashCode());
            CacheValue existingValue = getFromCache(cacheKey, hash);
            if (existingValue != null) {
                result.add(existingValue.offset);
            } else {
                result.add(TableKey.NOT_EXISTS);
                toLookup.put(hash, new LookupInfo(cacheKey, i));
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
                            LookupInfo lookupInfo = toLookup.get(keyHash);
                            assert lookupInfo != null : "Unable to locate LookupInfo in toLookup Map based on KeyHash";

                            // Cache the bucket's location, but only if its path is complete.
                            CacheValue newestEntry = null;
                            if (!bucket.isPartial()) {
                                newestEntry = updateCacheForKey(lookupInfo.cacheKey, keyHash, new CacheValue(this.indexReader.getOffset(bucket.getLastNode())));
                            }

                            result.set(lookupInfo.resultOffset, newestEntry == null ? TableKey.NOT_EXISTS : newestEntry.offset);
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
        return this.indexReader.getBackpointerOffset(offset, segment, timeout);
    }

    /**
     * Performs a Batch Update.
     *
     * If {@link UpdateBatch#isConditional()} returns true, this will execute an atomic Conditional Update based on the
     * condition items in the batch ({@link UpdateBatch#versionedItems}. The entire UpdateBatch will be conditioned on
     * those items, including those items that do not have a condition set. The entire UpdateBatch will either all be
     * committed as one unit or not at all.
     *
     * Otherwise this will perform an Unconditional Update, where all the Update Items will be applied regardless of
     * whether they already exist or what their versions are.
     *
     * @param segment The Segment to perform update on.
     * @param batch   The UpdateBatch to apply.
     * @param persist A Supplier that, when invoked, will persist the contents of the batch to the Segment and return a
     *                CompletableFuture to indicate when the operation is done, containing the offset at which the batch
     *                has been written.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain a list of offsets (within the Segment) where each
     * of the items in the batch has been persisted. If the update failed, it will be failed with the appropriate exception.
     * Notable exceptions:
     * <ul>
     * <li>{@link KeyNotExistsException} If a Key in the UpdateBatch does not exist and was conditioned as having to exist.
     * <li>{@link BadKeyVersionException} If a Key does exist but had a version mismatch.
     * </ul>
     */
    CompletableFuture<List<Long>> update(DirectSegmentAccess segment, UpdateBatch batch, Supplier<CompletableFuture<Long>> persist, TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        if (batch.isConditional()) {
            // Conditional update.
            // Collect all Cache Keys for the Update Items that have a condition on them; we need this on order to
            // serialize execution across them.
            val keys = batch.versionedItems.stream()
                    .map(item -> new CacheKey(segment.getSegmentId(), item.hash.hashCode()))
                    .collect(Collectors.toList());

            // Serialize the execution (queue it up to run only after all other currently queued up conditional updates
            // for touched keys have finished).
            return this.conditionalUpdateProcessor.add(
                    keys,
                    () -> validateConditionalUpdate(segment, batch, timer)
                            .thenComposeAsync(v -> persist.get(), this.executor)
                            .thenApplyAsync(batchOffset -> updateCacheForBatch(batch, segment.getSegmentId(), batchOffset), this.executor));
        } else {
            // Unconditional update: persist the entries and update the cache.
            return persist.get().thenApply(batchOffset -> updateCacheForBatch(batch, segment.getSegmentId(), batchOffset));
        }
    }


    /**
     * Performs a Batch Removal.
     *
     * If {@link UpdateBatch#isConditional()} returns true, this will execute an atomic Conditional Removal based on the
     * condition items in the batch ({@link UpdateBatch#versionedItems}. The entire UpdateBatch will be conditioned on
     * those items, including those items that do not have a condition set. The entire UpdateBatch will either all be
     * removed as one unit or not at all.
     *
     * Otherwise this will perform an Unconditional Removal, where all the Update Items will be applied regardless of
     * whether they already exist or what their versions are.
     *
     * @param segment The Segment to perform removal on.
     * @param batch   The UpdateBatch to remove.
     * @param persist A Supplier that, when invoked, will persist the contents of the batch to the Segment and return a
     *                CompletableFuture to indicate when the operation is done, containing the offset at which the batch
     *                has been written.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain a list of offsets (within the Segment) where each
     * of the items in the batch has been persisted. If the update failed, it will be failed with the appropriate exception.
     * Notable exceptions:
     * <ul>
     * <li>{@link KeyNotExistsException} If a Key in the UpdateBatch does not exist and was conditioned as having to exist.
     * <li>{@link BadKeyVersionException} If a Key does exist but had a version mismatch.
     * </ul>
     */
    CompletableFuture<List<Long>> remove(DirectSegmentAccess segment, UpdateBatch batch, Supplier<CompletableFuture<Long>> persist, TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        if (batch.isConditional()) {
            // Conditional removal.
            // Collect all Cache Keys for the Update Items that have a condition on them; we need this on order to
            // serialize execution across them.
            val keys = batch.versionedItems.stream()
                                           .map(item -> new CacheKey(segment.getSegmentId(), item.hash.hashCode()))
                                           .collect(Collectors.toList());

            // Serialize the execution (queue it up to run only after all other currently queued up conditional updates
            // for touched keys have finished).
            return this.conditionalUpdateProcessor.add(
                    keys,
                    () -> validateConditionalUpdate(segment, batch, timer)
                            .thenComposeAsync(v -> persist.get(), this.executor)
                            .thenApplyAsync(batchOffset -> updateCacheForBatch(batch, segment.getSegmentId(), batchOffset), this.executor));
        } else {
            // Unconditional removal: persist the entries and update the cache.
            return persist.get().thenApply(batchOffset -> updateCacheForBatch(batch, segment.getSegmentId(), batchOffset));
        }
    }

    /**
     * Validates all the conditional updates specified in the given UpdateBatch.
     *
     * @param segment The Segment to operate on.
     * @param batch   The UpdateBatch to validate.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will indicate that validation succeeded. If the validation did
     * not pass, it will be failed with the appropriate exception. Notable exceptions:
     * <ul>
     * <li>{@link KeyNotExistsException} If a Key does not exist and was conditioned as having to exist.
     * <li>{@link BadKeyVersionException} If a Key does exist but had a version mismatch.
     * </ul>
     */
    private CompletableFuture<Void> validateConditionalUpdate(DirectSegmentAccess segment, UpdateBatch batch, TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        List<KeyHash> hashes = batch.versionedItems.stream().map(i -> i.hash).collect(Collectors.toList());
        return getBucketOffsets(segment, hashes, timer)
                .thenAccept(offsets -> validateConditionalUpdate(batch.versionedItems, offsets, segment.getInfo().getName()));
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
    private void validateConditionalUpdate(List<UpdateBatchItem> items, List<Long> bucketOffsets, String segmentName) {
        assert items.size() == bucketOffsets.size() : "items.size() != bucketOffsets.size()";

        for (int i = 0; i < items.size(); i++) {
            // Validate compareVersion.
            TableKey key = items.get(i).key;
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

    /**
     * Updates the contents of one or more Cache Entries related to the given UpdateBatch. Each entry is updated only if
     * no previous entry exists with its key or if the new entry's offset is greater than the existing Entry's offset.
     *
     * @param batch       An UpdateBatch containing items whose Cache Entries need updating.
     * @param segmentId   Segment Id that the UpdateBatch Items belong to.
     * @param batchOffset Offset in the Segment where the first item in the UpdateBatch has been written to.
     * @return A List of offsets for each item in the UpdateBatch (in the same order) of where the latest value for that
     * item's Key exists now.
     */
    private List<Long> updateCacheForBatch(UpdateBatch batch, long segmentId, long batchOffset) {
        val result = new ArrayList<Supplier<Long>>(batch.items.size());
        synchronized (this.cacheEntries) {
            int generation = this.currentCacheGeneration;
            for (UpdateBatchItem item : batch.items) {
                CacheKey cacheKey = new CacheKey(segmentId, item.hash.hashCode());
                CacheEntry entry = this.cacheEntries.computeIfAbsent(cacheKey, key -> new CacheEntry(cacheKey, generation));
                result.add(() -> entry.updateIfNewer(item.hash, new CacheValue(batchOffset + item.offset), generation).getOffset());
            }
        }

        return result.stream().map(Supplier::get).collect(Collectors.toList());
    }

    /**
     * Updates the contents of a Cache Entry, but only if no previous entry exists or if the new Entry's offset is greater
     * than the existing Entry's offset.
     *
     * @param cacheKey The Cache Entry's Key.
     * @param newEntry The Cache Entry to insert/update.
     * @return The CacheEntry that exists in the Cache when this method exists. This will be newEntry or whatever Cache Entry
     * was there before (if that entry superseded the new one).
     */
    private CacheValue updateCacheForKey(CacheKey cacheKey, KeyHash keyHash, CacheValue newEntry) { // TODO: CacheValue -> long
        CacheEntry entry;
        int generation;
        synchronized (this.cacheEntries) {
            generation = this.currentCacheGeneration;
            entry = this.cacheEntries.getOrDefault(cacheKey, null);
            if (entry == null) {
                entry = new CacheEntry(cacheKey, generation);
                this.cacheEntries.put(cacheKey, entry);
            }
        }

        return entry.updateIfNewer(keyHash, newEntry, generation);
    }

    private CacheValue getFromCache(CacheKey cacheKey, KeyHash keyHash) {
        CacheEntry entry;
        int generation;
        synchronized (this.cacheEntries) {
            generation = this.currentCacheGeneration;
            entry = this.cacheEntries.getOrDefault(cacheKey, null);
        }

        return entry == null ? null : entry.get(keyHash, generation);
    }

    //endregion

    //region CacheKey

    /**
     * A key to access data in the Cache. A CacheKey is uniquely identified by a {SegmentId, KeyHashGroup} pair. Since
     * KeyHashes are too large to use as in-memory references for long, they are re-hashed into KeyHashGroups which are
     * simpler to manage.
     */
    private static class CacheKey extends Cache.Key {
        private static final int SERIALIZATION_LENGTH = Long.BYTES + Integer.BYTES;
        private final long segmentId;
        private final int keyHashGroup;

        CacheKey(long segmentId, int keyHashGroup) {
            this.segmentId = segmentId;
            this.keyHashGroup = keyHashGroup;
        }

        @Override
        public byte[] serialize() {
            byte[] result = new byte[SERIALIZATION_LENGTH];
            BitConverter.writeLong(result, 0, this.segmentId);
            BitConverter.writeInt(result, Long.BYTES, this.keyHashGroup);
            return result;
        }

        @Override
        public int hashCode() {
            return this.keyHashGroup; // KeyHashGroup is already a hash.
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CacheKey)) {
                return false;
            }

            CacheKey other = (CacheKey) obj;
            return this.segmentId == other.segmentId
                    && this.keyHashGroup == other.keyHashGroup;
        }
    }

    //endregion

    //region CacheEntry

    /**
     * An entry in the Cache to which one or more CacheValues are mapped.
     * Each CacheEntry is uniquely mapped to a {SegmentId, KeyHashGroup} pair (A KeyHashGroup is a hash of the KeyHash).
     * Each CacheEntry will contain a collection of {KeyHash, CacheValue} pairs, with the property that all KeyHashes in
     * a CacheEntry will have the same KeyHashGroup.
     */
    private class CacheEntry {
        private static final int HEADER_LENGTH = Integer.BYTES;
        private final CacheKey key;
        @GuardedBy("this")
        private int generation;
        @GuardedBy("this")
        private int size;
        @GuardedBy("this")
        private long highestOffset;

        CacheEntry(CacheKey key, int currentGeneration) {
            this.key = key;
            this.generation = currentGeneration;
            this.size = 0;
            this.highestOffset = 0;
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
         * Gets a value representing the Highest offset that is stored in any CacheValues in this CacheEntry.
         */
        synchronized long getHighestOffset() {
            return this.highestOffset;
        }

        /**
         * Looks up a CacheValue in this CacheEntry that is associated with the given KeyHash.
         *
         * @param keyHash           The KeyHash to look up.
         * @param currentGeneration The current Cache Generation (from the Cache Manager). The internal generation will
         *                          only be updated if at least one Attribute Value is fetched (cache hit).
         * @return A CacheValue representing the sought data, or null if nothing was found.
         */
        CacheValue get(KeyHash keyHash, int currentGeneration) {
            byte[] data = ContainerKeyIndex.this.cache.get(this.key);
            int offset = locate(keyHash, data);
            if (offset >= 0) {
                // Found it.
                synchronized (this) {
                    // Update Entry's generation.
                    this.generation = currentGeneration;
                }

                return CacheValue.deserialize(data, offset);
            }

            // Nothing found.
            return null;
        }

        /**
         * Inserts or updates the Cache for the given KeyHash with given CacheValue, but only if it has a higher offset
         * than the existing value.
         *
         * @param keyHash           The KeyHash to update.
         * @param value             The value to update.
         * @param currentGeneration The current Cache Generation (from the Cache Manager). The internal generation will
         *                          only be updated if at least one Attribute Value is updated.
         */
        synchronized CacheValue updateIfNewer(KeyHash keyHash, CacheValue value, int currentGeneration) {
            byte[] entryData = ContainerKeyIndex.this.cache.get(this.key);
            int offset = locate(keyHash, entryData);
            if (offset < 0) {
                // No match. Need to create a new array, copy any existing data and add new Cache Value.
                if (entryData == null) {
                    entryData = new byte[HEADER_LENGTH + keyHash.getLength() + CacheValue.SERIALIZATION_LENGTH];
                    offset = HEADER_LENGTH;
                } else {
                    byte[] newData;
                    newData = new byte[entryData.length + keyHash.getLength() + CacheValue.SERIALIZATION_LENGTH];
                    System.arraycopy(entryData, 0, newData, 0, entryData.length);
                    offset = entryData.length;
                    entryData = newData;
                }

                // Increment the count.
                int count = BitConverter.readInt(entryData, 0);
                BitConverter.writeInt(entryData, 0, count + 1);

                // Write the Key at the latest offset.
                keyHash.copyTo(entryData, offset, keyHash.getLength());
                offset += keyHash.getLength();
            } else {
                // We found a match. Verify if we can update it.
                CacheValue existingValue = CacheValue.deserialize(entryData, offset);
                if (value.offset < existingValue.offset) {
                    // New Value has lower offset than existing one. Nothing else to do.
                    return existingValue;
                }
            }

            // Update the value.
            value.serialize(entryData, offset);

            // Update the cache and stats.
            ContainerKeyIndex.this.cache.insert(this.key, entryData);
            this.size = entryData.length;
            this.generation = currentGeneration;
            this.highestOffset = Math.max(this.highestOffset, value.offset);
            return value;
        }

        /**
         * Locates the Offset of a CacheValue associated with the given KeyHash in the given Cache Entry data.
         *
         * @param keyHash The KeyHash to look up.
         * @param data    The array to look into.
         * @return The offset of the CacheValue associated with the KeyHash, or -1 if not found.
         */
        private int locate(KeyHash keyHash, byte[] data) {
            if (data != null && data.length > 0) {
                int count = BitConverter.readInt(data, 0);
                int offset = Integer.BYTES;
                byte[] keyHashArray = keyHash.array();
                int keyHashLength = keyHash.getLength();
                for (int i = 0; i < count; i++) {
                    // Check if the sought key matches the key at this index.
                    boolean match = true;
                    for (int j = 0; j < keyHashLength; j++) {
                        if (keyHashArray[keyHash.arrayOffset() + j] != data[offset + j]) {
                            match = false;
                            break;
                        }
                    }

                    if (match) {
                        return offset + keyHash.getLength(); // We return the offset of the value, not the key.
                    }

                    // No match; skip to the next entry.
                    offset += keyHash.getLength() + CacheValue.SERIALIZATION_LENGTH;
                }
            }

            return -1;
        }
    }

    //endregion

    //region Helper Classes


    @RequiredArgsConstructor
    private static class LookupInfo {
        final CacheKey cacheKey;
        final int resultOffset;
    }

    static class UpdateBatch {
        private final List<UpdateBatchItem> items = new ArrayList<>();
        private final List<UpdateBatchItem> versionedItems = new ArrayList<>();
        @Getter
        private int length;

        void add(TableKey key, KeyHash hash, int length) {
            UpdateBatchItem item = new UpdateBatchItem(key, hash, this.length);
            this.items.add(item);
            this.length += length;
            if (key.hasVersion()) {
                this.versionedItems.add(item);
            }
        }

        boolean isConditional() {
            return this.versionedItems.size() > 0;
        }
    }

    @RequiredArgsConstructor
    private static class UpdateBatchItem {
        private final TableKey key;
        private final KeyHash hash;
        private final int offset;
    }

    /**
     * Represents an actual Cache Value. This would map to a single {SegmentId, KeyHash} pair.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class CacheValue {
        private static final int SERIALIZATION_LENGTH = Long.BYTES;
        private final long offset;

        void serialize(byte[] target, int targetOffset) {
            BitConverter.writeLong(target, targetOffset, this.offset);
        }

        static CacheValue deserialize(byte[] input, int inputOffset) {
            return new CacheValue(BitConverter.readLong(input, inputOffset));
        }

        @Override
        public synchronized String toString() {
            return String.format("Offset = %d", this.offset);
        }
    }

    //endregion
}