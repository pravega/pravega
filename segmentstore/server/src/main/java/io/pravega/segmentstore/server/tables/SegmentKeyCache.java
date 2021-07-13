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
import com.google.common.base.Preconditions;
import io.pravega.common.hash.HashHelper;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Represents a Key Cache for a Table Segment.
 *
 * The Cache is organized in two different sections:
 * - Tail Section: Contains all new updates to the Table Segment that have not yet been indexed (Bucket offsets and Backpointers)
 * - Index Section: Contains cached Bucket offsets (no backpointers) for anything that has been indexed
 *
 * The two sections are delimited by the Segment's Last Indexed Offset ({@link #setLastIndexedOffset}. When this value
 * changes, eligible items are migrated from the Tail Section into the Index Section. Eligible items are bucket offsets
 * that are before this Last Indexed Offset and which do not point to Bucket deletions.
 *
 * The cache is separated in this manner because the Tail Section is optimized to bear the brunt of all Index Modifications
 * to a Table Segment; all updates and removals will end up modifying this section directly, so it is important that it
 * provides an easily modifiable data structure. The Index Section is designed for less frequent updates but it can handle
 * a larger amount of data being cached (since it is backed by the process-wide {@link CacheStorage}). The Tail Section, while
 * dynamic, is not expected to grow too large due to the Table Segment being continuously indexed in the background, which
 * causes the Last Indexed Offset to be updated frequently.
 */
@ThreadSafe
@RequiredArgsConstructor
@Slf4j
class SegmentKeyCache {
    //region Members
    private static final int HASH_GROUP_COUNT = 1024;
    private static final HashHelper HASH = HashHelper.seededWith(SegmentKeyCache.class.getName());
    private static final int VALUE_SERIALIZATION_LENGTH = Long.BYTES; // CacheBucketOffset serializes to a Long.

    @Getter
    private final long segmentId;
    private final CacheStorage cacheStorage;
    @Setter
    private volatile boolean essentialCacheOnly;
    @GuardedBy("this")
    private long lastIndexedOffset;
    @GuardedBy("this")
    private final HashMap<Long, Long> backpointers = new HashMap<>();
    @GuardedBy("this")
    private final HashMap<Short, CacheEntry> cacheEntries = new HashMap<>(); // Index Cache: KeyHashGroup -> CacheEntry
    @GuardedBy("this")
    private final HashMap<UUID, CacheBucketOffset> tailOffsets = new HashMap<>(); // Tail Cache: Key Hash -> Hash Offset

    //endregion

    //region Cache Management

    /**
     * Generates a {@link CacheManager.CacheStatus} containing the current state of the Cache for this Segment.
     *
     * @return A new {@link CacheManager.CacheStatus} instance.
     */
    synchronized CacheManager.CacheStatus getCacheStatus() {
        return CacheManager.CacheStatus.fromGenerations(
                this.cacheEntries.values().stream().filter(Objects::nonNull).map(CacheEntry::getGeneration).iterator());
    }

    /**
     * Collects and unregisters all Cache Entries with a generation smaller than the given one. This method does not
     * actually execute the eviction since it is invoked while a lock is held in {@link ContainerKeyCache}. The caller
     * ({@link ContainerKeyCache}) needs to execute the actual cache eviction.
     *
     * @param oldestGeneration The oldest permissible generation.
     * @return A List of {@link CacheEntry} instances representing the evicted entries.
     */
    synchronized List<CacheEntry> evictBefore(int oldestGeneration) {
        // Remove those entries that have a generation below the oldest permissible one.
        ArrayList<CacheEntry> removedEntries = new ArrayList<>();
        for (val e : this.cacheEntries.entrySet()) {
            CacheEntry entry = e.getValue();
            if (entry.getGeneration() < oldestGeneration
                    && entry.getHighestOffset() < this.lastIndexedOffset) {
                removedEntries.add(entry);
            }
        }

        // Clear the expired cache entries.
        removedEntries.forEach(e -> this.cacheEntries.remove(e.hashGroup));
        return removedEntries;
    }

    /**
     * Same as {@link #evictBefore}, but removes all Cache Entries.
     *
     * @return See {@link #evictBefore}
     */
    synchronized List<CacheEntry> evictAll() {
        // Remove those entries that have a generation below the oldest permissible one.
        val entries = new ArrayList<>(this.cacheEntries.values());
        this.cacheEntries.clear();
        return entries;
    }

    //endregion

    //region Operations

    /**
     * Updates the tail cache for with the contents of the given {@link TableKeyBatch}.
     * Each {@link TableKeyBatch.Item} is updated only if no previous entry exists with its {@link TableKeyBatch.Item#getHash()}
     * or if its {@link TableKeyBatch.Item#getOffset()} is greater than the existing entry's offset.
     *
     * This method should be used for processing new updates to the Index (as opposed from bulk-loading already indexed keys).
     *
     * @param batch       An {@link TableKeyBatch} containing items to accept into the Cache.
     * @param batchOffset Offset in the Segment where the first item in the {@link TableKeyBatch} has been written to.
     * @param generation  The current Cache Generation (from the Cache Manager).
     * @return A List of offsets for each item in the {@link TableKeyBatch} (in the same order) of where the latest value
     * for that item's Key exists now.
     */
    List<Long> includeUpdateBatch(TableKeyBatch batch, long batchOffset, int generation) {
        val result = new ArrayList<Long>(batch.getItems().size());
        synchronized (this) {
            for (TableKeyBatch.Item item : batch.getItems()) {
                long itemOffset = batchOffset + item.getOffset();
                CacheBucketOffset existingOffset = get(item.getHash(), generation);
                if (existingOffset == null || itemOffset > existingOffset.getSegmentOffset()) {
                    // We have no previous entry, or we do and the current offset is higher, so it prevails.
                    this.tailOffsets.put(item.getHash(), new CacheBucketOffset(itemOffset, batch.isRemoval()));
                    result.add(itemOffset);
                } else {
                    // Current offset is lower.
                    result.add(existingOffset.getSegmentOffset());
                }

                if (existingOffset != null) {
                    // Only record a backpointer if we have a previous location to point to.
                    this.backpointers.put(itemOffset, existingOffset.getSegmentOffset());
                }
            }
        }

        return result;
    }

    /**
     * Updates the tail cache with the given data, which represents a pre-index result of the tail section of the Segment.
     *
     * @param keyOffsets A Map of KeyHashes to {@link CacheBucketOffset} instances that represents the latest values (including
     *                   deletions) for all the pre-indexed keys).
     * @param generation The current Cache Generation (from the Cache Manager).
     */
    synchronized void includeTailCache(Map<UUID, CacheBucketOffset> keyOffsets, int generation) {
        for (val e : keyOffsets.entrySet()) {
            CacheBucketOffset offset = e.getValue();
            CacheBucketOffset existingOffset = get(e.getKey(), generation);
            if (existingOffset == null || offset.getSegmentOffset() > existingOffset.getSegmentOffset()) {
                // We have no previous entry, or we do and the current offset is higher, so it prevails.
                this.tailOffsets.put(e.getKey(), offset);
            }
        }
    }

    /**
     * Updates the contents of a Cache Entry associated with the given Segment Id and KeyHash. This method cannot be
     * used to remove values.
     *
     * This method should be used for processing existing keys (that have already been indexed), as opposed from processing
     * new (un-indexed) keys.
     *
     * @param keyHash       A UUID representing the Key Hash to look up.
     * @param segmentOffset The segment offset where this Key has its latest value.
     * @return Either segmentOffset, or the offset which contains the most up-to-date information about this KeyHash.
     * If this value does not equal segmentOffset, it means some other concurrent update changed this value, and that
     * value prevailed. This value could be negative (see segmentOffset doc).
     */
    long includeExistingKey(UUID keyHash, long segmentOffset, int generation) {
        Preconditions.checkArgument(segmentOffset >= 0, "segmentOffset must be non-negative.");
        short hashGroup = getHashGroup(keyHash);
        CacheEntry entry;
        synchronized (this) {
            CacheBucketOffset tailOffset = this.tailOffsets.get(keyHash);
            if (tailOffset != null && tailOffset.getSegmentOffset() >= segmentOffset) {
                // There already exists a higher offset for this Key Hash. No need to do more.
                return tailOffset.getSegmentOffset();
            }

            entry = this.cacheEntries.computeIfAbsent(hashGroup, hg -> new CacheEntry(hashGroup, generation));
        }

        // Update the cache entry directly.
        if (!entry.update(keyHash, segmentOffset, generation)) {
            evictEntry(entry);

        }
        return segmentOffset;
    }

    private void evictEntry(CacheEntry entry) {
        synchronized (this) {
            this.cacheEntries.remove(entry.hashGroup);
        }

        entry.evict(); // This need not be invoked while holding the lock (it has its own synchronization).
    }

    /**
     * Looks up a cached offset for the given Key Hash.
     *
     * @param keyHash A UUID representing the Key Hash to look up.
     * @return A {@link CacheBucketOffset} representing the sought result.
     */
    CacheBucketOffset get(UUID keyHash, int generation) {
        CacheEntry entry;
        synchronized (this) {
            // First, check the tail cache.
            CacheBucketOffset tailOffset = this.tailOffsets.get(keyHash);
            if (tailOffset != null) {
                return tailOffset;
            }

            entry = this.cacheEntries.get(getHashGroup(keyHash));
        }

        // Check the Cache Entry.
        if (entry != null) {
            Long r = entry.get(keyHash, generation);
            if (r != null) {
                return CacheBucketOffset.decode(r);
            }
        }

        return null;
    }

    /**
     * Updates the Last Indexed Offset (cached value of the Segment's {@link TableAttributes#INDEX_OFFSET} attribute).
     * Clears out any backpointers whose source offsets will be smaller than the new value for Last Indexed Offset.
     */
    void setLastIndexedOffset(long currentLastIndexedOffset, int cacheGeneration) {
        val candidates = new ArrayList<MigrationCandidate>();
        synchronized (this) {
            // Update stored last indexed offset.
            Preconditions.checkArgument(currentLastIndexedOffset >= this.lastIndexedOffset,
                    "currentLastIndexedOffset (%s) must be at least the current value (%s).",
                    currentLastIndexedOffset, this.lastIndexedOffset);
            this.lastIndexedOffset = currentLastIndexedOffset;

            // Remove obsolete backpointers. They now exist in the index.
            this.backpointers.keySet().removeIf(sourceOffset -> sourceOffset < currentLastIndexedOffset);
            for (val tailHash : this.tailOffsets.entrySet()) {
                val offset = tailHash.getValue();
                if (offset.getSegmentOffset() < currentLastIndexedOffset) {
                    // This entry has already been indexed, so it should be removed from the tail cache.
                    CacheEntry cacheEntry = this.cacheEntries.computeIfAbsent(getHashGroup(tailHash.getKey()),
                            hg -> new CacheEntry(hg, cacheGeneration));
                    candidates.add(new MigrationCandidate(tailHash.getKey(), cacheEntry, offset));
                }
            }
        }

        candidates.forEach(mc -> commitMigrationCandidate(mc, cacheGeneration));
        synchronized (this) {
            // Finally, remove tail hashes, but ONLY if they haven't changed - it's possible that since we released the lock
            // above a newer value was recorded; we shouldn't be removing it then. We use Map.remove(Key, Value) for this.
            candidates.forEach(c -> this.tailOffsets.remove(c.keyHash, c.offset));
        }
    }

    private void commitMigrationCandidate(MigrationCandidate mc, int cacheGeneration) {
        if (!mc.commit(cacheGeneration)) {
            // If we were unable to commit a migration candidate, we should evict it. It likely is in an inconsistent state.
            // This is safe to do since any data in this entry can be reloaded from the index.
            evictEntry(mc.cacheEntry);
        }
    }

    /**
     * Gets the Last Indexed Offset.
     */
    synchronized long getLastIndexedOffset() {
        return this.lastIndexedOffset;
    }

    /**
     * Gets a backpointer from the given sourceOffset, or -1 if no such link exists.
     */
    synchronized long getBackpointerOffset(long sourceOffset) {
        return this.backpointers.getOrDefault(sourceOffset, -1L);
    }

    /**
     * Gets a list of all Tail Entry Hashes mapped to their offsets.
     */
    synchronized Map<UUID, CacheBucketOffset> getTailBucketOffsets() {
        return new HashMap<>(this.tailOffsets);
    }

    /**
     * Gets a number representing the expected change in number of entries to the index once all the tail cache entries
     * are included in it.
     *
     * @return The tail entry update count delta.
     */
    synchronized int getTailEntryCountDelta() {
        return this.tailOffsets.values().stream().mapToInt(o -> o.isRemoval() ? -1 : 1).sum();
    }

    @Override
    public synchronized String toString() {
        return String.format("LIO = %s, Entries = %s, Backpointers = %s, BucketOffsets = %s.",
                this.lastIndexedOffset, this.cacheEntries.size(), this.backpointers.size(), this.tailOffsets.size());
    }

    private short getHashGroup(UUID keyHash) {
        return (short) HASH.hashToBucket(keyHash, HASH_GROUP_COUNT);
    }

    //endregion

    //region Helper Classes

    /**
     * Represents a candidate for Migration from the Tail Cache to the Index Cache.
     */
    @RequiredArgsConstructor
    @VisibleForTesting
    static class MigrationCandidate {
        /**
         * Key Hash to migrate.
         */
        final UUID keyHash;
        /**
         * Target Cache Entry to migrate into.
         */
        final CacheEntry cacheEntry;
        /**
         * Offset in Segment.
         */
        final CacheBucketOffset offset;

        /**
         * Commits this {@link MigrationCandidate} into the base {@link CacheEntry}.
         *
         * @param cacheGeneration The current Cache Generation.
         * @return True if the commit was a success; false otherwise (if the cache entry has been evicted in the meantime).
         */
        boolean commit(int cacheGeneration) {
            try {
                return this.cacheEntry.update(this.keyHash, this.offset.encode(), cacheGeneration);
            } catch (CacheEntryEvictedException ex) {
                // Nothing to do here. A concurrent eviction has removed this entry so there isn't much more we can do.
                return false;
            }
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
    class CacheEntry {
        private static final int HEADER_LENGTH = Integer.BYTES;
        private static final int HASH_LENGTH = KeyHasher.HASH_SIZE_BYTES;
        private static final int ENTRY_LENGTH = HEADER_LENGTH + HASH_LENGTH + VALUE_SERIALIZATION_LENGTH;
        private static final int INITIAL_ADDRESS = -1;
        private static final int EVICTED_ADDRESS = -2;
        private final short hashGroup;
        private volatile int generation;
        private volatile long highestOffset;
        @GuardedBy("this")
        private int cacheAddress;

        private CacheEntry(short hashGroup, int currentGeneration) {
            this.hashGroup = hashGroup;
            this.generation = currentGeneration;
            this.highestOffset = 0;
            this.cacheAddress = INITIAL_ADDRESS;
        }

        /**
         * Gets a value representing the current Generation of this Cache Entry. This value is updated every time the
         * data behind this entry is modified or accessed.
         */
        int getGeneration() {
            return this.generation;
        }

        /**
         * Gets a value representing the Highest offset that is stored in any CacheValues in this CacheEntry.
         */
        long getHighestOffset() {
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
        Long get(UUID keyHash, int currentGeneration) {
            ArrayView data = getFromCache();
            int offset = locate(keyHash, data);
            if (offset >= 0) {
                // Found it.
                // Update Entry's generation.
                this.generation = currentGeneration;
                return data.getLong(offset);
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
         */
        synchronized boolean update(UUID keyHash, long segmentOffset, int currentGeneration) {
            ArrayView entryData = getFromCache();
            int entryOffset = locate(keyHash, entryData);
            if (entryOffset < 0) {
                // No match. Need to create a new array, copy any existing data and add new Cache Value.
                if (entryData == null) {
                    entryData = new ByteArraySegment(new byte[ENTRY_LENGTH]);
                    entryOffset = HEADER_LENGTH;
                } else {
                    ByteArraySegment newData = new ByteArraySegment(new byte[entryData.getLength() + HASH_LENGTH + VALUE_SERIALIZATION_LENGTH]);
                    newData.copyFrom(entryData, 0, entryData.getLength());
                    entryOffset = entryData.getLength();
                    entryData = newData;
                }

                // Increment the count.
                entryData.setInt(0, entryData.getInt(0) + 1);

                // Write the Key Hash at the latest offset.
                serializeHash(entryData, entryOffset, keyHash);
                entryOffset += HASH_LENGTH;
            }

            // Update the value.
            entryData.setLong(entryOffset, segmentOffset);

            // Update the cache and stats.
            try {
                storeInCache(entryData);
                this.generation = currentGeneration;
                this.highestOffset = Math.max(this.highestOffset, segmentOffset);
            } catch (CacheEntryEvictedException cex) {
                // Pass-through exception. If this is the case, the entry has already been evicted so not more we can do.
                throw cex;
            } catch (CacheDisabledException cex) {
                log.debug("SegmentKeyCache[{}]: Not updating cache for {} due to non-essential cache entries disabled.", segmentId, this);
                return false;
            } catch (Throwable ex) {
                // There is nothing we can do here. Invoke the general handler to remove this from the cache.
                log.warn("SegmentKeyCache[{}]: Cache Entry update failed for {}.", segmentId, this, ex);
                return false;
            }

            return true;
        }

        /**
         * Removes the contents of this entry from the cache, if anything was stored there in the first place. Invoking
         * this method will cause {@link #storeInCache} to throw an {@link IllegalStateException} going forward.
         *
         * @return True if there was anything evicted, false otherwise.
         */
        synchronized boolean evict() {
            int address = this.cacheAddress;
            this.cacheAddress = EVICTED_ADDRESS;
            if (address >= 0) {
                SegmentKeyCache.this.cacheStorage.delete(address);
                return true;
            }

            return false;
        }

        private synchronized ArrayView getFromCache() {
            BufferView data = null;
            if (this.cacheAddress >= 0) {
                data = SegmentKeyCache.this.cacheStorage.get(this.cacheAddress);
            }
            return data == null ? null : new ByteArraySegment(data.getCopy());
        }

        @GuardedBy("this")
        private void storeInCache(ArrayView data) {
            if (this.cacheAddress == EVICTED_ADDRESS) {
                throw new CacheEntryEvictedException();
            }

            if (SegmentKeyCache.this.essentialCacheOnly) {
                // All our entries are considered "non-essential" (they can all be regenerated from persisted data).
                // If we shouldn't insert these, and if we need to update an existing one, we need to remove it so that
                // we don't serve stale data to upstream code.
                if (this.cacheAddress >= 0) {
                    SegmentKeyCache.this.cacheStorage.delete(this.cacheAddress);
                    this.cacheAddress = EVICTED_ADDRESS;
                }

                throw new CacheDisabledException(); // This is handled upstream.
            }

            if (this.cacheAddress >= 0) {
                this.cacheAddress = SegmentKeyCache.this.cacheStorage.replace(this.cacheAddress, data);
            } else {
                this.cacheAddress = SegmentKeyCache.this.cacheStorage.insert(data);
            }
        }

        /**
         * Locates the offset of the given Key Hash in the given Cache Entry data.
         *
         * @param keyHash The Key Hash to look up.
         * @param data    The array to look into.
         * @return The offset of the CacheValue associated with the KeyHash, or -1 if not found.
         */
        private int locate(UUID keyHash, ArrayView data) {
            if (data != null && data.getLength() > 0) {
                final int count = data.getInt(0);
                int offset = Integer.BYTES;

                for (int i = 0; i < count; i++) {
                    // Check if the sought key matches the key at this index.
                    boolean match = keyHash.getMostSignificantBits() == data.getLong(offset)
                            && keyHash.getLeastSignificantBits() == data.getLong(offset + Long.BYTES);
                    if (match) {
                        return offset + HASH_LENGTH; // We return the offset of the value, not the key.
                    }

                    // No match; skip to the next entry.
                    offset += HASH_LENGTH + VALUE_SERIALIZATION_LENGTH;
                }
            }

            return -1;
        }

        private void serializeHash(ArrayView target, int targetOffset, UUID hash) {
            target.setLong(targetOffset, hash.getMostSignificantBits());
            target.setLong(targetOffset + Long.BYTES, hash.getLeastSignificantBits());
        }

        @Override
        public String toString() {
            return String.format("Key = %s", this.hashGroup);
        }
    }

    private static class CacheEntryEvictedException extends IllegalStateException {
        CacheEntryEvictedException() {
            super("CacheEntry evicted.");
        }
    }

    private static class CacheDisabledException extends IllegalStateException {
        CacheDisabledException() {
            super("Cache disabled for non-essential data.");
        }
    }

    //endregion
}