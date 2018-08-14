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
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.btree.BTreeIndex;
import io.pravega.common.util.btree.PageEntry;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class SegmentAttributeBTreeIndex implements AttributeIndex, CacheManager.Client, AutoCloseable {
    //region Members
    private static final int KEY_LENGTH = 2 * Long.BYTES; // UUID
    private static final int VALUE_LENGTH = Long.BYTES;
    private final SegmentMetadata segmentMetadata;
    private final AtomicReference<SegmentHandle> handle;
    private final Storage storage;
    private final Cache cache;
    @GuardedBy("cacheEntries")
    private int currentCacheGeneration;
    @GuardedBy("cacheEntries")
    private final Map<Long, CacheEntry> cacheEntries;

    private final BTreeIndex index;
    private final AttributeIndexConfig config;
    private final ScheduledExecutorService executor;
    private final String traceObjectId;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor and Initialization

    SegmentAttributeBTreeIndex(@NonNull SegmentMetadata segmentMetadata, @NonNull Storage storage, @NonNull Cache cache,
                               @NonNull AttributeIndexConfig config, @NonNull ScheduledExecutorService executor) {
        this.segmentMetadata = segmentMetadata;
        this.storage = storage;
        this.cache = cache;
        this.config = config;
        this.executor = executor;
        this.handle = new AtomicReference<>();
        this.index = BTreeIndex.builder()
                .keyLength(KEY_LENGTH)
                .valueLength(VALUE_LENGTH)
                .maxPageSize(8 * 1024) // TODO: config
                .executor(this.executor)
                .getLength(this::getLength)
                .readPage(this::readPage)
                .writePages(this::writePages)
                .build();

        this.cacheEntries = new HashMap<>();
        this.traceObjectId = String.format("AttributeIndex[%s]", this.segmentMetadata.getId());
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
        Preconditions.checkState(!this.index.isInitialized(), "SegmentAttributeIndex is already initialized.");
        String attributeSegmentName = StreamSegmentNameUtils.getAttributeSegmentName(this.segmentMetadata.getName());

        // Attempt to open the Attribute Segment; if it does not exist yet then create it.
        return Futures
                .exceptionallyComposeExpecting(
                        this.storage.openWrite(attributeSegmentName).thenAccept(this.handle::set),
                        ex -> ex instanceof StreamSegmentNotExistsException,
                        () -> this.storage.create(attributeSegmentName, this.config.getAttributeSegmentRollingPolicy(), timer.getRemaining())
                                .thenComposeAsync(
                                        si -> this.storage.openWrite(attributeSegmentName).thenAccept(this.handle::set),
                                        this.executor))
                .thenComposeAsync(v -> this.index.initialize(timer.getRemaining()), this.executor)
                .thenRun(() -> log.debug("{}: Initialized.", this.traceObjectId));
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
            entries = new ArrayList<>(this.cacheEntries.values());
            this.cacheEntries.clear();
        }

        removeFromCache(entries);
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
        synchronized (this.cacheEntries) {
            this.currentCacheGeneration = currentGeneration;
            ArrayList<CacheEntry> toRemove = new ArrayList<>();
            for (val entry : this.cacheEntries.values()) {
                if (entry.getGeneration() < oldestGeneration) {
                    sizeRemoved += entry.getSize();
                    toRemove.add(entry);
                }
            }

            removeFromCache(toRemove);
        }

        return sizeRemoved;
    }

    //endregion

    //region AttributeIndex Implementation

    @Override
    public CompletableFuture<Void> put(@NonNull Map<UUID, Long> values, @NonNull Duration timeout) {
        ensureInitialized();
        if (values.isEmpty()) {
            // Nothing to do.
            return CompletableFuture.completedFuture(null);
        }

        val entries = values.entrySet().stream().map(this::serialize).collect(Collectors.toList());
        return Futures.toVoid(this.index.put(entries, timeout));
    }

    @Override
    public CompletableFuture<Map<UUID, Long>> get(@NonNull Collection<UUID> keys, @NonNull Duration timeout) {
        ensureInitialized();
        if (keys.isEmpty()) {
            // Nothing to do.
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        // Keep two lists, one of keys (in some order) and one of serialized keys (in the same order).
        val keyList = new ArrayList<UUID>(keys.size());
        val serializedKeys = new ArrayList<ByteArraySegment>(keyList.size());
        for (UUID key : keys) {
            keyList.add(key);
            serializedKeys.add(serializeKey(key));
        }

        return this.index.get(serializedKeys, timeout)
                .thenApply(entries -> {
                    // The index search result is a list of values in the same order as the keys we passed in, so we need
                    // to use the list index to match them.
                    HashMap<UUID, Long> result = new HashMap<>();
                    for (int i = 0; i < keyList.size(); i++) {
                        ByteArraySegment v = entries.get(i);
                        if(v != null) {
                            result.put(keyList.get(i), deserializeValue(v));
                        }
                    }

                    return result;
                });
    }

    @Override
    public CompletableFuture<Void> remove(@NonNull Collection<UUID> keys, @NonNull Duration timeout) {
        ensureInitialized();
        if (keys.isEmpty()) {
            // Nothing to do.
            return CompletableFuture.completedFuture(null);
        }

        val serializedKeys = keys.stream().map(this::serializeKey).collect(Collectors.toList());
        return Futures.toVoid(this.index.remove(serializedKeys, timeout));
    }

    @Override
    public CompletableFuture<Void> seal(@NonNull Duration timeout) {
        ensureInitialized();
        return Futures.exceptionallyExpecting(
                this.storage.seal(this.handle.get(), timeout)
                        .thenRun(() -> log.info("{}: Sealed.", this.traceObjectId)),
                ex -> ex instanceof StreamSegmentSealedException,
                null);
    }

    //endregion

    private void ensureInitialized() {
        Preconditions.checkState(this.index.isInitialized(), "SegmentAttributeIndex is not initialized.");
    }

    private PageEntry serialize(Map.Entry<UUID, Long> entry) {
        byte[] value = new byte[VALUE_LENGTH];
        BitConverter.writeLong(value, 0, entry.getValue());

        return new PageEntry(serializeKey(entry.getKey()), new ByteArraySegment(value));
    }

    private ByteArraySegment serializeKey(UUID key) {
        byte[] result = new byte[KEY_LENGTH];
        BitConverter.writeLong(result, 0, key.getMostSignificantBits());
        BitConverter.writeLong(result, Long.BYTES, key.getLeastSignificantBits());
        return new ByteArraySegment(result);
    }

    private long deserializeValue(ByteArraySegment value) {
        Preconditions.checkArgument(value.getLength() == VALUE_LENGTH, "Unexpected value length.");
        return BitConverter.readLong(value, 0);
    }

    private CompletableFuture<Long> getLength(Duration timeout) {
        return this.storage.getStreamSegmentInfo(this.handle.get().getSegmentName(), timeout)
                .thenApply(SegmentProperties::getLength);
    }

    private CompletableFuture<ByteArraySegment> readPage(long offset, int length, Duration timeout) {
        // First, check in the cache.
        byte[] fromCache = getFromCache(offset, length);
        if (fromCache != null) {
            return CompletableFuture.completedFuture(new ByteArraySegment(fromCache));
        }

        // Cache miss; load data from Storage.
        byte[] buffer = new byte[length];
        return this.storage.read(this.handle.get(), offset, buffer, 0, length, timeout)
                .thenApplyAsync(bytesRead -> {
                    Preconditions.checkArgument(length == bytesRead, "Unexpected number of bytes read.");
                    storeInCache(offset, buffer);
                    return new ByteArraySegment(buffer);
                }, this.executor);
    }

    private CompletableFuture<Long> writePages(List<Map.Entry<Long, ByteArraySegment>> pages, Collection<Long> obsoleteOffsets, Duration timeout) {
        // Collect the data to be written.
        val streams = new ArrayList<InputStream>();
        AtomicLong offset = new AtomicLong(-1);
        AtomicInteger length = new AtomicInteger();
        for (val e : pages) {
            long pageOffset = e.getKey();
            ByteArraySegment pageContents = e.getValue();
            if (!offset.compareAndSet(-1, pageOffset)) {
                Preconditions.checkArgument(pageOffset == offset.get() + length.get(), "Unexpected page offset.");
            }

            streams.add(pageContents.getReader());
            length.addAndGet(pageContents.getLength());
        }

        // Write a footer with information about locating the root page.

        // Write it.
        val toWrite = new SequenceInputStream(Collections.enumeration(streams));
        return this.storage.write(this.handle.get(), offset.get(), toWrite, length.get(), timeout)
                .thenApplyAsync(v -> {
                    // Store data in cache and remove obsolete pages.
                    storeInCache(pages, obsoleteOffsets);

                    // Return the current length of the Segment Attribute Index.
                    return offset.get() + length.get();
                }, this.executor);
    }

    private byte[] getFromCache(long offset, int length) {
        synchronized (this.cacheEntries) {
            CacheEntry entry = this.cacheEntries.getOrDefault(offset, null);
            if (entry != null) {
                byte[] data = this.cache.get(entry.getKey());
                if (data != null && data.length == length) {
                    entry.setGeneration(this.currentCacheGeneration);
                    return data;
                }
            }
        }

        return null;
    }

    private void storeInCache(long offset, byte[] data) {
        synchronized (this.cacheEntries) {
            CacheEntry entry = this.cacheEntries.getOrDefault(offset, null);
            if (entry == null || entry.getSize() != data.length) {
                entry = new CacheEntry(offset, data.length, this.currentCacheGeneration);
                this.cacheEntries.put(offset, entry);
            }

            this.cache.insert(entry.getKey(), data);
        }
    }

    private void storeInCache(List<Map.Entry<Long, ByteArraySegment>> toAdd, Collection<Long> obsoleteOffsets) {
        synchronized (this.cacheEntries) {
            // Remove obsolete pages.
            obsoleteOffsets.stream()
                    .map(this.cacheEntries::get)
                    .filter(Objects::nonNull)
                    .forEach(this::removeFromCache);

            // Add new ones.
            for (val e : toAdd) {
                long offset = e.getKey();
                ByteArraySegment data = e.getValue();
                CacheEntry entry = this.cacheEntries.getOrDefault(offset, null);
                if (entry == null || entry.getSize() != data.getLength()) {
                    entry = new CacheEntry(offset, data.getLength(), this.currentCacheGeneration);
                    this.cacheEntries.put(offset, entry);
                }

                this.cache.insert(entry.getKey(), data);
            }
        }
    }

    private void removeFromCache(Collection<CacheEntry> entries) {
        synchronized (this.cacheEntries) {
            entries.forEach(this::removeFromCache);
        }
    }

    @GuardedBy("cacheEntries")
    private void removeFromCache(CacheEntry e) {
        this.cache.remove(e.getKey());
        this.cacheEntries.remove(e.getOffset());
    }

    //region CacheEntry

    /**
     * An entry in the Cache to which one or more Attributes are mapped.
     */
    private class CacheEntry {
        /**
         * Id of the entry. This is used to lookup cached data in the Cache.
         */
        @Getter
        private final long offset;
        @Getter
        private final int size;
        @GuardedBy("this")
        private int generation;

        CacheEntry(long offset, int size, int currentGeneration) {
            this.offset = offset;
            this.size = size;
            this.generation = currentGeneration;
        }

        /**
         * Gets a new CacheKey representing this Entry.
         */
        CacheKey getKey() {
            return new CacheKey(SegmentAttributeBTreeIndex.this.segmentMetadata.getId(), this.offset);
        }

        /**
         * Gets a value representing the current Generation of this Cache Entry. This value is updated every time the
         * data behind this entry is modified or accessed.
         */
        synchronized int getGeneration() {
            return this.generation;
        }

        synchronized void setGeneration(int value) {
            this.generation = value;
        }
    }

    //endregion


}
