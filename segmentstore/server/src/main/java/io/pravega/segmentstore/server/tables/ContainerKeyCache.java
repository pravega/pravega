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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.CacheFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Data;
import lombok.NonNull;
import lombok.val;

/**
 * Cache Operations for {@link ContainerKeyIndex}.
 */
class ContainerKeyCache implements CacheManager.Client, AutoCloseable {
    //region Members

    private static final int VALUE_SERIALIZATION_LENGTH = Long.BYTES;
    private final Cache cache;
    @GuardedBy("cacheEntries")
    private final Map<CacheKey, CacheEntry> cacheEntries;
    @GuardedBy("cacheEntries")
    private final Map<Long, SegmentIndexTail> indexTails;
    @GuardedBy("cacheEntries")
    private int currentCacheGeneration;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerKeyCache class.
     *
     * @param containerId  The Id of the SegmentContainer that this instance is associated with.
     * @param cacheFactory A {@link CacheFactory} that can be used to create {@link Cache} instances.
     */
    ContainerKeyCache(int containerId, @NonNull CacheFactory cacheFactory) {
        this.cache = cacheFactory.getCache(String.format("Container_%d_TableKeys", containerId));
        this.cacheEntries = new HashMap<>();
        this.indexTails = new HashMap<>();
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
                this.indexTails.clear();
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
                SegmentIndexTail tail = this.indexTails.getOrDefault(e.getKey().segmentId, null);
                long lastIndexedOffset = tail == null ? -1L : tail.getLastIndexedOffset();
                if (lastIndexedOffset >= 0L
                        && entry.getHighestOffset() < lastIndexedOffset
                        && entry.getGeneration() < oldestGeneration) {
                    toRemove.add(e.getKey());
                    sizeRemoved += entry.getSize();
                } else {
                    remainingSegmentIds.add(e.getKey().segmentId);
                }
            }

            // Clear the expired cache entries.
            toRemove.forEach(this.cacheEntries::remove);

            // Remove those segment offset caches that are no longer used.
            this.indexTails.keySet().removeIf(segmentId -> !remainingSegmentIds.contains(segmentId));
        }

        // Remove from the Cache. It's ok to do this outside of the lock as the cache is thread safe.
        toRemove.forEach(this.cache::remove);
        return sizeRemoved;
    }

    //endregion

    //region Cache Operations

    /**
     * Updates the contents of one or more Cache Entries related to the given {@link TableKeyBatch}. Each entry is updated
     * only if no previous entry exists with its key or if the new entry's offset is greater than the existing Entry's offset.
     *
     * This method should be used for processing new updates to the Index (as opposed from bulk-loading already indexed keys).
     *
     * @param segmentId   Segment Id that the TableKeyBatch Items belong to.
     * @param batch       An TableKeyBatch containing items whose Cache Entries need updating.
     * @param batchOffset Offset in the Segment where the first item in the TableKeyBatch has been written to.
     * @return A List of offsets for each item in the TableKeyBatch (in the same order) of where the latest value for that
     * item's Key exists now. See {@link #includeExistingKey} return doc for interpreting these values.
     */
    List<Long> includeUpdateBatch(long segmentId, TableKeyBatch batch, long batchOffset) {
        val result = new ArrayList<Supplier<Long>>(batch.getItems().size());
        synchronized (this.cacheEntries) {
            int generation = this.currentCacheGeneration;
            for (TableKeyBatch.Item item : batch.getItems()) {
                // Calculate the CacheEntry offset, by factoring in the batch's offset.
                long cacheSegmentOffset = encodeValue(batchOffset + item.getOffset(), batch.isRemoval());
                CacheKey cacheKey = new CacheKey(segmentId, item.getHash());
                CacheEntry entry = this.cacheEntries.computeIfAbsent(cacheKey, key -> new CacheEntry(cacheKey, generation));

                // Queue up the update (so we process it outside of this lock), which should also update backpointers as well.
                result.add(() -> updateEntry(segmentId, entry, item.getHash(), cacheSegmentOffset, generation));
            }
        }

        // Update the Cache Entries outside of the main lock (they are thread safe), and calculate the results.
        return result.stream().map(Supplier::get).collect(Collectors.toList());
    }

    /**
     * Updates the contents of a Cache Entry associated with the given Segment Id and KeyHash, but only if the new value
     * for segmentOffset supersedes (is higher) than the existing value (or if no existing value). This method cannot be
     * used to remove values.
     *
     * This method should be used for processing existing keys (that have already been indexed), as opposed from processing
     * new (un-indexed) keys.
     *
     * @param segmentId     The Segment Id.
     * @param keyHash       A UUID representing the Key Hash to look up.
     * @param segmentOffset The segment offset where this Key has its latest value. If positive, it indicates that the latest
     *                      value for the key is located here. If negative, it indicates that the Key has been removed and
     *                      the absolute value of this argument indicates the offset where the Key's removal is recorded.
     * @return Either segmentOffset, or the offset which contains the most up-to-date information about this KeyHash.
     * If this value does not equal segmentOffset, it means some other concurrent update changed this value, and that
     * value prevailed. This value could be negative (see segmentOffset doc).
     */
    long includeExistingKey(long segmentId, UUID keyHash, long segmentOffset) {
        Preconditions.checkArgument(segmentOffset >= 0, "segmentOffset must be non-negative.");
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

        // Update the entry directly, without touching backpointers. This method is used for already-indexed keys, so
        // the backpointers should already be part of the index.
        return entry.updateIfNewer(keyHash, segmentOffset, generation).getCurrentOffset();
    }

    /**
     * Looks up a cached offset for the given Segment.
     *
     * @param segmentId The Id of the Segment to look up for.
     * @param keyHash   A UUID representing the Key Hash to look up.
     * @return A {@link GetResult} representing the sought result.
     */
    GetResult get(long segmentId, UUID keyHash) {
        CacheKey key = new CacheKey(segmentId, keyHash);
        CacheEntry entry;
        int generation;
        synchronized (this.cacheEntries) {
            generation = this.currentCacheGeneration;
            entry = this.cacheEntries.getOrDefault(key, null);
        }

        return entry == null ? null : entry.get(keyHash, generation);
    }

    /**
     * Updates the Last Indexed Offset for a given Segment. This is used for cache eviction purposes - no cache entry with
     * a segment offsets smaller than this value may be evicted. A Segment must be registered either via this method or
     * via {@link #updateSegmentIndexOffsetIfMissing} in order to have backpointers recorded for the tail-end section of
     * the index.
     *
     * @param segmentId   The Id of the Segment to update the Last Indexed Offset for.
     * @param indexOffset The Last Indexed Offset to set. If negative, this will clear up the value.
     */
    void updateSegmentIndexOffset(long segmentId, long indexOffset) {
        synchronized (this.cacheEntries) {
            if (indexOffset < 0) {
                this.indexTails.remove(segmentId);
            } else {
                SegmentIndexTail tail = this.indexTails.getOrDefault(segmentId, null);
                if (tail == null) {
                    this.indexTails.put(segmentId, new SegmentIndexTail(indexOffset));
                } else {
                    tail.setLastIndexedOffset(indexOffset);
                }
            }
        }
    }

    /**
     * Updates the Last Indexed Offset for a given Segment, but only if there currently isn't any information about that.
     * See {@link #updateSegmentIndexOffset(long, long)} for more details.
     *
     * @param segmentId         The Id of the Segment to update the Last Indexed Offset for.
     * @param indexOffsetGetter A Supplier that is only invoked if there is no information about the current segment. This
     *                          Supplier should return the current value of the Segment's Last Indexed Offset.
     */
    void updateSegmentIndexOffsetIfMissing(long segmentId, Supplier<Long> indexOffsetGetter) {
        SegmentIndexTail tail;
        synchronized (this.cacheEntries) {
            tail = this.indexTails.getOrDefault(segmentId, null);
        }

        if (tail == null) {
            tail = new SegmentIndexTail(indexOffsetGetter.get());
            synchronized (this.cacheEntries) {
                this.indexTails.put(segmentId, tail);
            }
        }
    }

    /**
     * Gets the value of the Last Indexed Offset for a Segment.
     *
     * @param segmentId The Id of the Segment to get the Last Indexed Offset for.
     * @return The Last Indexed Offset for the Segment, or -1 if this segment is not registered.
     */
    long getSegmentIndexOffset(long segmentId) {
        SegmentIndexTail tail;
        synchronized (this.cacheEntries) {
            tail = this.indexTails.getOrDefault(segmentId, null);
        }

        return tail == null ? -1 : tail.getLastIndexedOffset();
    }

    /**
     * Gets the Backpointer offset from the given one, if recorded.
     *
     * @param segmentId    The Id of the Segment to get Backpointer for.
     * @param sourceOffset The origin of the Backpointer.
     * @return The target of the Backpointer (from the given source), or -1 if no such Backpointer is registered.
     */
    long getBackpointer(long segmentId, long sourceOffset) {
        SegmentIndexTail tail;
        synchronized (this.cacheEntries) {
            tail = this.indexTails.getOrDefault(segmentId, null);
        }

        return tail == null ? -1 : tail.getBackpointerOffset(sourceOffset);
    }

    /**
     * Gets the unindexed Key Hashes, mapped to their latest offsets.
     *
     * @param segmentId The Id of the Segment to get Hashes for.
     * @return The result.
     */
    Map<UUID, Long> getTailHashes(long segmentId) {
        SegmentIndexTail tail;
        synchronized (this.cacheEntries) {
            tail = this.indexTails.getOrDefault(segmentId, null);
        }

        return tail == null ? Collections.emptyMap() : tail.getEntries();
    }

    /**
     * Records a new Tail Entry (Backpointer and Hash) for the given segment, but only if the Segment is registered
     * (via {@link #updateSegmentIndexOffset} or {@link #updateSegmentIndexOffsetIfMissing} and only if offset is beyond
     * the Segment's Last Indexed Offset.
     *
     * @param segmentId      The Id of the Segment to record the Tail Entry.
     * @param offset         The Entry's offset.
     * @param previousOffset The previous offset for this Entry.
     * @param itemHash       A UUID representing the Entry's Key Hash.
     */
    @VisibleForTesting
    void recordTailEntry(long segmentId, long offset, long previousOffset, UUID itemHash) {
        SegmentIndexTail tail;
        synchronized (this.cacheEntries) {
            tail = this.indexTails.getOrDefault(segmentId, null);
        }

        if (tail != null) {
            tail.recordTailEntry(offset, previousOffset, itemHash);
        }
    }

    private long updateEntry(long segmentId, CacheEntry entry, UUID itemHash, long cacheSegmentOffset, int generation) {
        UpdateCacheResult ucr = entry.updateIfNewer(itemHash, cacheSegmentOffset, generation);
        recordTailEntry(segmentId, ucr.getCurrentOffset(), ucr.getPreviousOffset(), itemHash);
        return ucr.getCurrentOffset();
    }

    private long encodeValue(long segmentOffset, boolean isRemoval) {
        return isRemoval ? -(segmentOffset + 1) : segmentOffset;
    }

    private GetResult decodeValue(long value) {
        return value < 0 ? new GetResult(-value - 1, false) : new GetResult(value, true);
    }

    //endregion

    //region CacheKey and CacheEntry

    /**
     * A key to access data in the Cache. A CacheKey is uniquely identified by a {SegmentId, KeyHashGroup} pair. Since
     * KeyHashes are too large to use as in-memory references for long, they are re-hashed into KeyHashGroups which are
     * simpler to manage.
     */
    static class CacheKey extends Cache.Key {
        private static final int SERIALIZATION_LENGTH = Long.BYTES + Integer.BYTES;
        private final long segmentId;
        private final int keyHashGroup;

        CacheKey(long segmentId, UUID hash) {
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

    /**
     * An entry in the Cache to which one or more CacheValues are mapped.
     * Each CacheEntry is uniquely mapped to a {SegmentId, KeyHashGroup} pair (A KeyHashGroup is a hash of the KeyHash).
     * Each CacheEntry will contain a collection of {KeyHash, CacheValue} pairs, with the property that all KeyHashes in
     * a CacheEntry will have the same KeyHashGroup.
     */
    private class CacheEntry {
        private static final int HEADER_LENGTH = Integer.BYTES;
        private static final int HASH_LENGTH = KeyHasher.HASH_SIZE_BYTES;
        private static final int ENTRY_LENGTH = HEADER_LENGTH + HASH_LENGTH + VALUE_SERIALIZATION_LENGTH;
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
         * @param keyHash           The Key Hash to look up.
         * @param currentGeneration The current Cache Generation (from the Cache Manager). The internal generation will
         *                          only be updated if at least one Attribute Value is fetched (cache hit).
         * @return See {@link ContainerKeyCache#get} return doc.
         */
        GetResult get(UUID keyHash, int currentGeneration) {
            byte[] data = ContainerKeyCache.this.cache.get(this.key);
            int offset = locate(keyHash, data);
            if (offset >= 0) {
                // Found it.
                synchronized (this) {
                    // Update Entry's generation.
                    this.generation = currentGeneration;
                }

                return decodeValue(deserializeCacheValue(data, offset));
            }

            // Nothing found.
            return null;
        }

        /**
         * Inserts or updates the Cache for the given KeyHash with given CacheValue, but only if it has a higher offset
         * than the existing value.
         *
         * @param keyHash           The Key Hash to update.
         * @param segmentOffset     See {@link ContainerKeyCache#includeExistingKey} segmentOffset.
         * @param currentGeneration The current Cache Generation (from the Cache Manager).
         * @return See {@link ContainerKeyCache#includeExistingKey} return doc.
         */
        synchronized UpdateCacheResult updateIfNewer(UUID keyHash, long segmentOffset, int currentGeneration) {
            val decodedSegmentOffset = decodeValue(segmentOffset);
            byte[] entryData = ContainerKeyCache.this.cache.get(this.key);
            int entryOffset = locate(keyHash, entryData);
            GetResult existingOffset = null;
            if (entryOffset < 0) {
                // No match. Need to create a new array, copy any existing data and add new Cache Value.
                if (entryData == null) {
                    entryData = new byte[ENTRY_LENGTH];
                    entryOffset = HEADER_LENGTH;
                } else {
                    byte[] newData;
                    newData = new byte[entryData.length + HASH_LENGTH + VALUE_SERIALIZATION_LENGTH];
                    System.arraycopy(entryData, 0, newData, 0, entryData.length);
                    entryOffset = entryData.length;
                    entryData = newData;
                }

                // Increment the count.
                int count = BitConverter.readInt(entryData, 0);
                BitConverter.writeInt(entryData, 0, count + 1);

                // Write the Key Hash at the latest offset.
                entryOffset += serializeHash(entryData, entryOffset, keyHash);
            } else {
                // We found a match. Verify if we can update it (needs to be newer than an offset for an existing entry).
                existingOffset = decodeValue(deserializeCacheValue(entryData, entryOffset));
                if (decodedSegmentOffset.getSegmentOffset() < existingOffset.getSegmentOffset() && existingOffset.isPresent()) {
                    // New offset is lower than existing one. Nothing else to do.
                    return new UpdateCacheResult(existingOffset.getSegmentOffset(), -1);
                }
            }

            // Update the value.
            serializeCacheValue(segmentOffset, entryData, entryOffset);

            // Update the cache and stats.
            ContainerKeyCache.this.cache.insert(this.key, entryData);
            this.size = entryData.length;
            this.generation = currentGeneration;
            this.highestOffset = Math.max(this.highestOffset, decodedSegmentOffset.getSegmentOffset());
            return new UpdateCacheResult(
                    decodedSegmentOffset.getSegmentOffset(),
                    existingOffset == null ? -1 : existingOffset.getSegmentOffset());
        }

        /**
         * Locates the offset of the given {@link HashedArray} in the given Cache Entry data.
         *
         * @param keyHash The Key Hash to look up.
         * @param data    The array to look into.
         * @return The offset of the CacheValue associated with the KeyHash, or -1 if not found.
         */
        private int locate(UUID keyHash, byte[] data) {
            if (data != null && data.length > 0) {
                int count = BitConverter.readInt(data, 0);
                int offset = Integer.BYTES;
                byte[] keyHashArray = new byte[HASH_LENGTH];
                serializeHash(keyHashArray, 0, keyHash);
                for (int i = 0; i < count; i++) {
                    // Check if the sought key matches the key at this index.
                    boolean match = true;
                    for (int j = 0; j < HASH_LENGTH; j++) {
                        if (keyHashArray[j] != data[offset + j]) {
                            match = false;
                            break;
                        }
                    }

                    if (match) {
                        return offset + HASH_LENGTH; // We return the offset of the value, not the key.
                    }

                    // No match; skip to the next entry.
                    offset += HASH_LENGTH + VALUE_SERIALIZATION_LENGTH;
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

        private int serializeHash(byte[] target, int targetOffset, UUID hash) {
            BitConverter.writeLong(target, targetOffset, hash.getMostSignificantBits());
            BitConverter.writeLong(target, targetOffset + Long.BYTES, hash.getLeastSignificantBits());
            return 2 * Long.BYTES;
        }
    }

    //endregion

    //region Other Helper classes

    /**
     * Result from {@link #get(long, UUID)}.
     */
    @Data
    static class GetResult {
        /**
         * The offset within the Segment where the latest update (or removal) for the sought key was located.
         * This value may not be accurate if {@link #isPresent()} is false.
         */
        private final long segmentOffset;
        /**
         * True if the Key is present in the cache, false otherwise.
         */
        private final boolean present;
    }

    /**
     * Result from the {@link CacheEntry#updateIfNewer} method.
     */
    @Data
    private static class UpdateCacheResult {
        /**
         * The current Segment Offset of the updated Key Hash.
         */
        final long currentOffset;

        /**
         * The previous Segment Offset of the updated Key Hash, or -1 if it wasn't cached.
         */
        final long previousOffset;
    }

    /**
     * Caches information about the tail-end section of the Index which cannot be effectively stored in the {@link Cache)}
     * due to the frequency of updates, fragmentation of information and lookup techniques.
     * <p>
     * The tail-end section of a Segment Index represents the byte range between the {@link Attributes#TABLE_INDEX_OFFSET}
     * and the end of the Segment (essentially whatever the back-end Writer Table Processor hasn't indexed yet).
     */
    @ThreadSafe
    private static class SegmentIndexTail {
        @GuardedBy("this")
        private long lastIndexedOffset;
        @GuardedBy("this")
        private final HashMap<Long, Long> backpointers;
        @GuardedBy("this")
        private final HashMap<UUID, Long> entries;

        SegmentIndexTail(long currentLastIndexedOffset) {
            Preconditions.checkArgument(currentLastIndexedOffset >= 0, "currentLastIndexedOffset must be a non-negative number.");
            this.lastIndexedOffset = currentLastIndexedOffset;
            this.backpointers = new HashMap<>();
            this.entries = new HashMap<>();
        }

        /**
         * Updates the Last Indexed Offset (cached value of the Segment's {@link Attributes#TABLE_INDEX_OFFSET} attribute).
         * Clears out any backpointers whose source offsets will be smaller than the new value for Last Indexed Offset.
         */
        synchronized void setLastIndexedOffset(long currentLastIndexedOffset) {
            Preconditions.checkArgument(currentLastIndexedOffset >= this.lastIndexedOffset,
                    "currentLastIndexedOffset must be at least the current value");
            this.lastIndexedOffset = currentLastIndexedOffset;
            this.backpointers.keySet().removeIf(sourceOffset -> sourceOffset < currentLastIndexedOffset);
            this.entries.values().removeIf(offset -> offset < currentLastIndexedOffset);
        }

        /**
         * Gets the Last Indexed Offset.
         */
        synchronized long getLastIndexedOffset() {
            return this.lastIndexedOffset;
        }

        /**
         * Records a new backpointer between the two offsets, but only if the sourceOffset is beyond {@link #getLastIndexedOffset()}.
         */
        synchronized void recordTailEntry(long offset, long previousOffset, UUID itemHash) {
            Preconditions.checkArgument(offset > previousOffset, "offset must be greater than previousOffset");
            if (offset >= this.lastIndexedOffset) {
                if (previousOffset >= 0) {
                    this.backpointers.put(offset, previousOffset);
                }

                this.entries.put(itemHash, offset);
            }
        }

        /**
         * Gets a backpointer from the given sourceOffset, or -1 if no such link exists.
         */
        synchronized long getBackpointerOffset(long sourceOffset) {
            return this.backpointers.getOrDefault(sourceOffset, -1L);
        }

        /**
         * Gets a list of all Entry Hashes in the this Index Tail.
         */
        synchronized Map<UUID, Long> getEntries() {
            return new HashMap<>(this.entries);
        }

        @Override
        public synchronized String toString() {
            return String.format("LIO = %s, Backpointers = %s, Entries = %s.", this.lastIndexedOffset, this.backpointers.size(), this.entries.size());
        }
    }

    //endregion
}
