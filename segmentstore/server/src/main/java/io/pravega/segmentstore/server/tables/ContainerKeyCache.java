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

import io.pravega.common.Exceptions;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import lombok.NonNull;
import lombok.val;

/**
 * Cache Operations for {@link ContainerKeyIndex}.
 */
class ContainerKeyCache implements CacheManager.Client, AutoCloseable {
    //region Members

    private final CacheStorage cacheStorage;
    @GuardedBy("segmentCaches")
    private final Map<Long, SegmentKeyCache> segmentCaches;
    @GuardedBy("segmentCaches")
    private int currentCacheGeneration;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerKeyCache class.
     *
     * @param cacheStorage A {@link CacheStorage} that can be used to store data in memory.
     */
    ContainerKeyCache(@NonNull CacheStorage cacheStorage) {
        this.cacheStorage = cacheStorage;
        this.segmentCaches = new HashMap<>();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            ArrayList<SegmentKeyCache> toEvict;
            synchronized (this.segmentCaches) {
                toEvict = new ArrayList<>(this.segmentCaches.values());
                this.segmentCaches.clear();
            }

            toEvict.forEach(s -> s.evictAll().forEach(SegmentKeyCache.CacheEntry::evict));
        }
    }

    //endregion

    //region CacheManager.Client Implementation

    @Override
    public CacheManager.CacheStatus getCacheStatus() {
        synchronized (this.segmentCaches) {
            return CacheManager.CacheStatus.combine(
                    this.segmentCaches.values().stream()
                                      .filter(Objects::nonNull)
                                      .map(SegmentKeyCache::getCacheStatus)
                                      .iterator());
        }
    }

    @Override
    public boolean updateGenerations(int currentGeneration, int oldestGeneration, boolean essentialOnly) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        // Instruct each Segment Cache to perform its own cache management, collect eviction candidates, and remove them
        // from the cache.
        val evictions = new ArrayList<SegmentKeyCache.CacheEntry>();
        synchronized (this.segmentCaches) {
            this.currentCacheGeneration = currentGeneration;
            for (SegmentKeyCache segmentCache : this.segmentCaches.values()) {
                segmentCache.setEssentialCacheOnly(essentialOnly);
                evictions.addAll(segmentCache.evictBefore(oldestGeneration));
            }
        }

        boolean anyEvicted = false;
        for (val e : evictions) {
            anyEvicted = e.evict() | anyEvicted;
        }

        return anyEvicted;
    }

    //endregion

    //region Cache Operations

    /**
     * Updates the tail cache for the given Table Segment with the contents of the given {@link TableKeyBatch}.
     * Each {@link TableKeyBatch.Item} is updated only if no previous entry exists with its {@link TableKeyBatch.Item#getHash()}
     * or if its {@link TableKeyBatch.Item#getOffset()} is greater than the existing entry's offset.
     *
     * This method should be used for processing new updates to the Index (as opposed from bulk-loading already indexed keys).
     *
     * @param segmentId   Segment Id that the {@link TableKeyBatch} Items belong to.
     * @param batch       An {@link TableKeyBatch} containing items to accept into the Cache.
     * @param batchOffset Offset in the Segment where the first item in the {@link TableKeyBatch} has been written to.
     * @return A List of offsets for each item in the {@link TableKeyBatch} (in the same order) of where the latest value
     * for that item's Key exists now.
     */
    List<Long> includeUpdateBatch(long segmentId, TableKeyBatch batch, long batchOffset) {
        SegmentKeyCache cache;
        int generation;
        synchronized (this.segmentCaches) {
            generation = this.currentCacheGeneration;
            cache = this.segmentCaches.computeIfAbsent(segmentId, s -> new SegmentKeyCache(s, this.cacheStorage));
        }

        return cache.includeUpdateBatch(batch, batchOffset, generation);
    }

    /**
     * Updates the tail cache for the given Table Segment with the given data, which represents a pre-index result of the
     * tail section of the Segment.
     *
     * @param segmentId  Segment Id.
     * @param keyOffsets A Map of KeyHashes to {@link CacheBucketOffset} instances that represents the latest values (including
     *                   deletions) for all the pre-indexed keys).
     */
    void includeTailCache(long segmentId, Map<UUID, CacheBucketOffset> keyOffsets) {
        SegmentKeyCache cache;
        int generation;
        synchronized (this.segmentCaches) {
            generation = this.currentCacheGeneration;
            cache = this.segmentCaches.computeIfAbsent(segmentId, s -> new SegmentKeyCache(s, this.cacheStorage));
        }

        cache.includeTailCache(keyOffsets, generation);
    }

    /**
     * Updates the contents of a Cache Entry associated with the given Segment Id and KeyHash. This method cannot be
     * used to remove values.
     *
     * This method should be used for processing existing keys (that have already been indexed), as opposed from processing
     * new (un-indexed) keys.
     *
     * @param segmentId     The Segment Id.
     * @param keyHash       A UUID representing the Key Hash to look up.
     * @param segmentOffset The segment offset where this Key has its latest value.
     * @return Either segmentOffset, or the offset which contains the most up-to-date information about this KeyHash.
     * If this value does not equal segmentOffset, it means some other concurrent update changed this value, and that
     * value prevailed. This value could be negative (see segmentOffset doc).
     */
    long includeExistingKey(long segmentId, UUID keyHash, long segmentOffset) {
        SegmentKeyCache cache;
        int generation;
        synchronized (this.segmentCaches) {
            generation = this.currentCacheGeneration;
            cache = this.segmentCaches.computeIfAbsent(segmentId, s -> new SegmentKeyCache(s, this.cacheStorage));
        }

        return cache.includeExistingKey(keyHash, segmentOffset, generation);
    }

    /**
     * Looks up a cached offset for the given Segment and Key Hash.
     *
     * @param segmentId The Id of the Segment to look up for.
     * @param keyHash   A UUID representing the Key Hash to look up.
     * @return A {@link CacheBucketOffset} representing the sought result.
     */
    CacheBucketOffset get(long segmentId, UUID keyHash) {
        SegmentKeyCache cache;
        int generation;
        synchronized (this.segmentCaches) {
            generation = this.currentCacheGeneration;
            cache = this.segmentCaches.get(segmentId);
        }

        return cache == null ? null : cache.get(keyHash, generation);
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
        boolean remove = indexOffset < 0;
        SegmentKeyCache cache;
        int generation;
        synchronized (this.segmentCaches) {
            generation = this.currentCacheGeneration;
            if (remove) {
                cache = this.segmentCaches.remove(segmentId);
            } else {
                cache = this.segmentCaches.computeIfAbsent(segmentId, s -> new SegmentKeyCache(s, this.cacheStorage));
            }
        }

        if (cache != null) {
            if (remove) {
                cache.evictAll().forEach(SegmentKeyCache.CacheEntry::evict);
            } else {
                cache.setLastIndexedOffset(indexOffset, generation);
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
        SegmentKeyCache cache = null;
        int generation;
        synchronized (this.segmentCaches) {
            generation = this.currentCacheGeneration;
            if (!this.segmentCaches.containsKey(segmentId)) {
                cache = new SegmentKeyCache(segmentId, this.cacheStorage);
            }
        }

        if (cache != null) {
            cache.setLastIndexedOffset(indexOffsetGetter.get(), generation);
        }
    }

    /**
     * Gets the value of the Last Indexed Offset for a Segment.
     *
     * @param segmentId The Id of the Segment to get the Last Indexed Offset for.
     * @return The Last Indexed Offset for the Segment, or -1 if this segment is not registered.
     */
    long getSegmentIndexOffset(long segmentId) {
        return forSegmentCache(segmentId, SegmentKeyCache::getLastIndexedOffset, -1L);
    }

    /**
     * Gets the Backpointer offset from the given one, if recorded.
     *
     * @param segmentId    The Id of the Segment to get Backpointer for.
     * @param sourceOffset The origin of the Backpointer.
     * @return The target of the Backpointer (from the given source), or -1 if no such Backpointer is registered.
     */
    long getBackpointer(long segmentId, long sourceOffset) {
        return forSegmentCache(segmentId, c -> c.getBackpointerOffset(sourceOffset), -1L);
    }

    /**
     * Gets the unindexed Key Hashes, mapped to their latest offsets.
     *
     * @param segmentId The Id of the Segment to get Hashes for.
     * @return The result.
     */
    Map<UUID, CacheBucketOffset> getTailHashes(long segmentId) {
        return forSegmentCache(segmentId, SegmentKeyCache::getTailBucketOffsets, Collections.emptyMap());
    }

    /**
     * Gets a number representing the expected change in number of entries to the index once all the tail cache entries
     * are included in it.
     *
     * @param segmentId The Id of the Segment to get the entry count delta.
     * @return The tail entry update count delta.
     */
    int getTailUpdateDelta(long segmentId) {
        return forSegmentCache(segmentId, SegmentKeyCache::getTailEntryCountDelta, 0);
    }

    private <T> T forSegmentCache(long segmentId, Function<SegmentKeyCache, T> ifExists, T ifNotExists) {
        SegmentKeyCache cache;
        synchronized (this.segmentCaches) {
            cache = this.segmentCaches.get(segmentId);
        }

        return cache == null ? ifNotExists : ifExists.apply(cache);
    }

    //endregion
}
