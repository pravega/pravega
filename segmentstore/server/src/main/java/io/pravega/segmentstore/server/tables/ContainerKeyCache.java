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
import io.pravega.common.util.BitConverter;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.CacheFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.NonNull;
import lombok.val;

class ContainerKeyCache implements CacheManager.Client, AutoCloseable {
    //region Members

    private static final int VALUE_SERIALIZATION_LENGTH = Long.BYTES;
    private final Cache cache;
    @GuardedBy("cacheEntries")
    private final Map<CacheKey, CacheEntry> cacheEntries;
    @GuardedBy("cacheEntries")
    private final Map<Long, Long> segmentIndexOffsets; // TODO: this needs updating once we wire up Writer Processors.
    @GuardedBy("cacheEntries")
    private int currentCacheGeneration;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    ContainerKeyCache(int containerId, @NonNull CacheFactory cacheFactory) {
        this.cache = cacheFactory.getCache(String.format("Container_%d_TableKeys", containerId));
        this.cacheEntries = new HashMap<>();
        this.segmentIndexOffsets = new HashMap<>();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.cache.close();
            synchronized (this.cacheEntries) {
                this.cacheEntries.clear();
                this.segmentIndexOffsets.clear();
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

    //region Cache Operations

    /**
     * Updates the contents of one or more Cache Entries related to the given TableKeyBatch. Each entry is updated only if
     * no previous entry exists with its key or if the new entry's offset is greater than the existing Entry's offset.
     *
     * @param segmentId   Segment Id that the TableKeyBatch Items belong to.
     * @param batch       An TableKeyBatch containing items whose Cache Entries need updating.
     * @param batchOffset Offset in the Segment where the first item in the TableKeyBatch has been written to.
     * @return A List of offsets for each item in the TableKeyBatch (in the same order) of where the latest value for that
     * item's Key exists now.
     */
    List<Long> updateBatch(long segmentId, TableKeyBatch batch, long batchOffset) {
        // TODO: handle removals.
        val result = new ArrayList<Supplier<Long>>(batch.getItems().size());
        synchronized (this.cacheEntries) {
            int generation = this.currentCacheGeneration;
            for (TableKeyBatch.Item item : batch.getItems()) {
                CacheKey cacheKey = new CacheKey(segmentId, item.getHash());
                CacheEntry entry = this.cacheEntries.computeIfAbsent(cacheKey, key -> new CacheEntry(cacheKey, generation));
                result.add(() -> entry.updateIfNewer(item.getHash(), batchOffset + item.getOffset(), generation));
            }
        }

        return result.stream().map(Supplier::get).collect(Collectors.toList());
    }

    /**
     * Updates the contents of a Cache Entry, but only if no previous entry exists or if the new Entry's offset is greater
     * than the existing Entry's offset.
     *
     * @param segmentId The Segment Id.
     * @param keyHash   The KeyHash to update for.
     * @param newOffset The Cache Entry's offset.
     * @return The CacheEntry that exists in the Cache when this method exists. This will be newEntry or whatever Cache Entry
     * was there before (if that entry superseded the new one).
     */
    Long updateKey(long segmentId, KeyHash keyHash, long newOffset) {
        CacheKey key = new CacheKey(segmentId, keyHash);
        CacheEntry entry;
        int generation;
        synchronized (this.cacheEntries) {
            generation = this.currentCacheGeneration;
            entry = this.cacheEntries.getOrDefault(key, null);
            if (entry == null) {
                entry = new CacheEntry(key, generation);
                this.cacheEntries.put(key, entry);
            }
        }

        return entry.updateIfNewer(keyHash, newOffset, generation);
    }

    Long get(long segmentId, KeyHash keyHash) {
        CacheKey key = new CacheKey(segmentId, keyHash);
        CacheEntry entry;
        int generation;
        synchronized (this.cacheEntries) {
            generation = this.currentCacheGeneration;
            entry = this.cacheEntries.getOrDefault(key, null);
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
    static class CacheKey extends Cache.Key {
        private static final int SERIALIZATION_LENGTH = Long.BYTES + Integer.BYTES;
        private final long segmentId;
        private final int keyHashGroup;

        CacheKey(long segmentId, KeyHash hash) {
            this.segmentId = segmentId;
            this.keyHashGroup = hash.hashCode();
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
        Long get(KeyHash keyHash, int currentGeneration) {
            byte[] data = ContainerKeyCache.this.cache.get(this.key);
            int offset = locate(keyHash, data);
            if (offset >= 0) {
                // Found it.
                synchronized (this) {
                    // Update Entry's generation.
                    this.generation = currentGeneration;
                }

                return deserializeCacheValue(data, offset);
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
        synchronized Long updateIfNewer(KeyHash keyHash, Long value, int currentGeneration) {
            byte[] entryData = ContainerKeyCache.this.cache.get(this.key);
            int offset = locate(keyHash, entryData);
            if (offset < 0) {
                // No match. Need to create a new array, copy any existing data and add new Cache Value.
                if (entryData == null) {
                    entryData = new byte[HEADER_LENGTH + keyHash.getLength() + VALUE_SERIALIZATION_LENGTH];
                    offset = HEADER_LENGTH;
                } else {
                    byte[] newData;
                    newData = new byte[entryData.length + keyHash.getLength() + VALUE_SERIALIZATION_LENGTH];
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
                Long existingValue = deserializeCacheValue(entryData, offset);
                if (value < existingValue) {
                    // New Value has lower offset than existing one. Nothing else to do.
                    return existingValue;
                }
            }

            // Update the value.
            serializeCacheValue(value, entryData, offset);

            // Update the cache and stats.
            ContainerKeyCache.this.cache.insert(this.key, entryData);
            this.size = entryData.length;
            this.generation = currentGeneration;
            this.highestOffset = Math.max(this.highestOffset, value);
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
                    offset += keyHash.getLength() + VALUE_SERIALIZATION_LENGTH;
                }
            }

            return -1;
        }

        private long deserializeCacheValue(byte[] input, int inputOffset) {
            return BitConverter.readLong(input, inputOffset);
        }

        private void serializeCacheValue(long value, byte[] target, int targetOffset) {
            BitConverter.writeLong(target, targetOffset, value);
        }
    }

    //endregion
}
