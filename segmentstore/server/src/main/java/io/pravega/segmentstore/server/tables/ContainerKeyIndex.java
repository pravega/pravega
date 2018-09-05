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
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.segmentstore.contracts.tables.ConditionalTableUpdateException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.CacheFactory;
import java.time.Duration;
import java.util.HashMap;
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
import lombok.val;

@ThreadSafe
class ContainerKeyIndex implements CacheManager.Client, AutoCloseable {
    //region Members

    @Getter
    private final Indexer indexer;
    private final ScheduledExecutorService executor;
    private final Cache cache;
    @GuardedBy("cacheEntries")
    private final Map<CacheKey, CacheEntry> cacheEntries;
    @GuardedBy("cacheEntries")
    private int currentCacheGeneration;
    private final SequentialAsyncQueue<CacheKey, Long> conditionalUpdateQueue;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    ContainerKeyIndex(int containerId, @NonNull CacheFactory cacheFactory, @NonNull ScheduledExecutorService executor) {
        this.cache = cacheFactory.getCache(String.format("Container_%d_TableKeys", containerId));
        this.executor = executor;
        this.indexer = new Indexer(executor);
        this.conditionalUpdateQueue = new SequentialAsyncQueue<>(this.executor);
        this.cacheEntries = new HashMap<>();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.cache.close();
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
        long indexedSegmentOffset = Long.MAX_VALUE; // TODO: this needs to be based on actual segment data (use Attributes.TABLE_INDEX_OFFSET).
        List<CacheKey> toRemove;
        synchronized (this.cacheEntries) {
            this.currentCacheGeneration = currentGeneration;
            toRemove = this.cacheEntries
                    .values().stream()
                    .filter(e -> e.getGeneration() < oldestGeneration && e.getHighestOffset() < indexedSegmentOffset)
                    .map(e -> e.key)
                    .collect(Collectors.toList());
            toRemove.forEach(this.cacheEntries::remove);
        }

        // Remove from the Cache. It's ok to do this outside of the lock as the cache is thread safe.
        toRemove.forEach(this.cache::remove);
        return sizeRemoved;
    }

    //endregion

    //region Operations

    /**
     * Find the Last Bucket Offset for the given KeyHash.
     *
     * @param segment Segment to look up Bucket Offset for.
     * @param hash    KeyHash to identify the Bucket.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the sought Offset, or null if this Bucket does not
     * exist.
     */
    CompletableFuture<Long> getBucketOffset(DirectSegmentAccess segment, KeyHash hash, TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        // Look it up in our cache.
        CacheKey cacheKey = new CacheKey(segment.getSegmentId(), hash.hashCode());
        CacheValue existingValue = getFromCache(cacheKey, hash);
        if (existingValue != null) {
            return CompletableFuture.completedFuture(existingValue.offset);
        }

        // If there's nothing in the cache, then look up the bucket and cache it. TODO wait on recovery if needed.
        return this.indexer
                .locateBucket(segment, hash, timer)
                .thenApplyAsync(bucket -> {
                    // Cache the bucket's location, but only if its path is complete.
                    TableBucket.Node last = bucket.getLastNode();
                    CacheValue newestEntry = null;
                    if (last != null && !last.isIndexNode()) {
                        newestEntry = updateCacheIfNewer(cacheKey, hash, new CacheValue(this.indexer.getOffset(last)));
                    }

                    return newestEntry == null ? null : newestEntry.offset;
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
        // TODO: maybe add this to the cache for quicker retrieval?
        return this.indexer.getBackpointerOffset(segment, offset, timeout);
    }

    /**
     * Executes a pass-through update.
     *
     * @param segment        The Segment to execute the update on.
     * @param hash           The KeyHash for the Key to update.
     * @param compareVersion (Optional) A CompareVersion for conditional updates.
     * @param persist        A Supplier that will return a CompletableFuture which will indicate when the entry has been
     *                       durably persisted.
     * @param timer          Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the offset at which the entry was added.
     */
    CompletableFuture<Long> update(DirectSegmentAccess segment, KeyHash hash, Long compareVersion, Supplier<CompletableFuture<Long>> persist, TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        CacheKey cacheKey = new CacheKey(segment.getSegmentId(), hash.hashCode());
        if (compareVersion == null) {
            // Blind update: persist the entry and update the cache.
            return persist.get().thenApply(offset -> {
                updateCacheIfNewer(cacheKey, hash, new CacheValue(offset));
                return offset;
            });
        } else {
            // Conditional update: queue up behind the last such update.
            return this.conditionalUpdateQueue.add(
                    cacheKey,
                    () -> getBucketOffset(segment, hash, timer)
                            .thenComposeAsync(bucketOffset -> {
                                Exceptions.checkNotClosed(this.closed.get(), this);

                                // Validate compareVersion.
                                if (!bucketOffset.equals(compareVersion)) {
                                    return Futures.failedFuture(new ConditionalTableUpdateException(segment.getInfo().getName(), null)); // TODO actual key
                                }

                                // Validation successful: persist the entry.
                                return persist.get();
                            }, this.executor)
                            .thenApplyAsync(persistedOffset -> {
                                // Update the cache.
                                updateCacheIfNewer(cacheKey, hash, new CacheValue(persistedOffset));
                                return persistedOffset;
                            }, this.executor));
        }
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
    private CacheValue updateCacheIfNewer(CacheKey cacheKey, KeyHash keyHash, CacheValue newEntry) {
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

    //region SequentialAsyncQueue

    /**
     * Concurrent async processor that allows parallel execution of tasks with different keys, but serializes the execution
     * of tasks with the same key.
     *
     * @param <KeyType>    Type of the Key.
     * @param <ReturnType> Return value of each async task.
     */
    @RequiredArgsConstructor
    private static class SequentialAsyncQueue<KeyType, ReturnType> {
        private final ScheduledExecutorService executor;
        @GuardedBy("queue")
        private final Map<KeyType, CompletableFuture<ReturnType>> queue = new HashMap<>();

        public CompletableFuture<ReturnType> add(KeyType key, Supplier<CompletableFuture<? extends ReturnType>> toRun) {
            CompletableFuture<ReturnType> result = new CompletableFuture<>();
            boolean newEntry = false;
            synchronized (this.queue) {
                CompletableFuture<ReturnType> existingTask = this.queue.getOrDefault(key, null);
                if (existingTask == null) {
                    // Nothing else currently; we'll need to add a new entry.
                    newEntry = true;
                } else {
                    // Another conditional update is in progress. Queue up behind it, and make sure to only start the
                    // execution once that update is completed.
                    existingTask.whenCompleteAsync((r, ex) -> Futures.completeAfter(toRun, result), this.executor);
                }

                // Update the queue to point to the latest task.
                this.queue.put(key, result);
            }

            if (newEntry) {
                // This was the first entry in this particular queue. Need to trigger its execution now, outside of the
                // synchronized block.
                Futures.completeAfter(toRun, result);
            }

            // Cleanup: if this was the last task in the queue, then clean up the queue.
            result.whenComplete((r, ex) -> {
                synchronized (this.queue) {
                    val last = this.queue.getOrDefault(key, null);
                    if (last != null && last.isDone()) {
                        this.queue.remove(key);
                    }
                }
            });

            return result;
        }
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
            byte[] keyData = keyHash.getArray();
            int offset = locate(keyHash, entryData);
            if (offset < 0) {
                // No match. Need to create a new array, copy any existing data and add new Cache Value.
                if (entryData == null) {
                    entryData = new byte[HEADER_LENGTH + keyData.length + CacheValue.SERIALIZATION_LENGTH];
                    offset = HEADER_LENGTH;
                } else {
                    byte[] newData;
                    newData = new byte[entryData.length + keyData.length + CacheValue.SERIALIZATION_LENGTH];
                    System.arraycopy(entryData, 0, newData, 0, entryData.length);
                    offset = entryData.length;
                    entryData = newData;
                }

                // Increment the count.
                int count = BitConverter.readInt(entryData, 0);
                BitConverter.writeInt(entryData, 0, count + 1);

                // Write the Key at the latest offset.
                System.arraycopy(keyData, 0, entryData, offset, keyData.length);
                offset += keyData.length;
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
                byte[] soughtKey = keyHash.getArray();
                for (int i = 0; i < count; i++) {
                    // Check if the sought key matches the key at this index.
                    boolean match = true;
                    for (int j = 0; j < soughtKey.length; j++) {
                        if (soughtKey[j] != data[offset + j]) {
                            match = false;
                            break;
                        }
                    }

                    if (match) {
                        return offset + soughtKey.length;
                    }

                    // No match; skip to the next entry.
                    offset += soughtKey.length + CacheValue.SERIALIZATION_LENGTH;
                }
            }

            return -1;
        }
    }

    //endregion

    //region CacheValue

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