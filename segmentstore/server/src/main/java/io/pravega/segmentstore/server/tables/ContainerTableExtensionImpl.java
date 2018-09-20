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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.tables.IteratorState;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.UpdateListener;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import io.pravega.segmentstore.server.tables.hashing.HashConfig;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import io.pravega.segmentstore.storage.CacheFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * A {@link ContainerTableExtension} that implements Table Segments on top of a {@link SegmentContainer}.
 */
public class ContainerTableExtensionImpl implements ContainerTableExtension {
    //region Members

    private static final HashConfig HASH_CONFIG = HashConfig.of(
            AttributeCalculator.PRIMARY_HASH_LENGTH,
            AttributeCalculator.SECONDARY_HASH_LENGTH,
            AttributeCalculator.SECONDARY_HASH_LENGTH,
            AttributeCalculator.SECONDARY_HASH_LENGTH,
            AttributeCalculator.SECONDARY_HASH_LENGTH);
    private static final int MAX_BATCH_SIZE = 32 * EntrySerializer.MAX_SERIALIZATION_LENGTH;
    private final SegmentContainer segmentContainer;
    private final CacheManager cacheManager;
    private final ScheduledExecutorService executor;
    private final KeyHasher hasher;
    private final ContainerKeyIndex keyIndex;
    private final EntrySerializer serializer;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerTableExtensionImpl class.
     *
     * @param segmentContainer The SegmentContainer to associate with.
     * @param cacheFactory     The CacheFactory to use in order to create Key Index Caches.
     * @param cacheManager     The CacheManager to use to manage the cache.
     * @param executor         An Executor to use for async tasks.
     */
    public ContainerTableExtensionImpl(@NonNull SegmentContainer segmentContainer, @NonNull CacheFactory cacheFactory,
                                       @NonNull CacheManager cacheManager, @NonNull ScheduledExecutorService executor) {
        this.segmentContainer = segmentContainer;
        this.cacheManager = cacheManager;
        this.executor = executor;
        this.hasher = KeyHasher.sha512(HASH_CONFIG);
        this.keyIndex = new ContainerKeyIndex(segmentContainer.getId(), cacheFactory, this.executor);
        this.cacheManager.register(this.keyIndex);
        this.serializer = new EntrySerializer();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.keyIndex.close();
            this.cacheManager.unregister(this.keyIndex);
        }
    }

    @Override
    public CompletableFuture<Void> initialize() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        if (!metadata.getAttributes().containsKey(Attributes.TABLE_NODE_ID)) {
            // Not a Table Segment; nothing to do.
            return Collections.emptyList();
        }

        return Collections.singletonList(new WriterTableProcessor(metadata, this.serializer, this.hasher,
                this.segmentContainer::forSegment, this.executor));
    }

    //endregion

    //region TableStore Implementation

    @Override
    public CompletableFuture<Void> createSegment(@NonNull String segmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.segmentContainer.createStreamSegment(segmentName,
                Collections.singleton(new AttributeUpdate(Attributes.TABLE_NODE_ID, AttributeUpdateType.None, 1)),
                timeout);
    }

    @Override
    public CompletableFuture<Void> deleteSegment(@NonNull String segmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.segmentContainer.deleteStreamSegment(segmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> merge(@NonNull String targetSegmentName, @NonNull String sourceSegmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException("merge");
    }

    @Override
    public CompletableFuture<Void> seal(String segmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException("seal");
    }

    @Override
    public CompletableFuture<List<Long>> put(@NonNull String segmentName, @NonNull List<TableEntry> entries, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        val batch = new ContainerKeyIndex.UpdateBatch();
        for (val e : entries) {
            int length = this.serializer.getUpdateLength(e);
            batch.add(e.getKey(), this.hasher.hash(e.getKey().getKey()), length);
        }

        Preconditions.checkArgument(batch.getLength() <= MAX_BATCH_SIZE, "Update Batch length (%s) exceeds the maximum limit.", MAX_BATCH_SIZE);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenCompose(segment -> this.keyIndex.update(segment, batch,
                        () -> {
                            // Serialize and commit the entire batch.
                            byte[] s = new byte[batch.getLength()];
                            this.serializer.serializeUpdate(entries, s);
                            return segment.append(s, null, timer.getRemaining());
                        }, timer));
    }

    @Override
    public CompletableFuture<Void> remove(@NonNull String segmentName, @NonNull Collection<TableKey> keys, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<List<TableEntry>> get(@NonNull String segmentName, @NonNull List<ArrayView> keys, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        } else {
            TimeoutTimer timer = new TimeoutTimer(timeout);
            List<KeyHash> hashes = keys.stream().map(this.hasher::hash).collect(Collectors.toList());
            return this.segmentContainer
                    .forSegment(segmentName, timer.getRemaining())
                    .thenComposeAsync(segment -> this.keyIndex.getBucketOffsets(segment, hashes, timer)
                                    .thenComposeAsync(offsets -> get(segment, keys, offsets, timer), this.executor),
                            this.executor);
        }
    }

    private CompletableFuture<List<TableEntry>> get(DirectSegmentAccess segment, List<ArrayView> keys, List<Long> offsets, TimeoutTimer timer) {
        List<CompletableFuture<TableEntry>> searchFutures = new ArrayList<>();
        for (int i = 0; i < offsets.size(); i++) {
            long offset = offsets.get(i);
            if (offset == TableKey.NOT_EXISTS) {
                // Key does not exist.
                searchFutures.add(CompletableFuture.completedFuture(null));
            } else {
                // Find the sought entry in the segment, based on its key.
                ArrayView key = keys.get(i);
                searchFutures.add(findEntry(segment, key, offset, timer)
                        .thenComposeAsync(entryInfo -> readEntry(segment, key, entryInfo, timer), this.executor));
            }
        }

        return Futures.allOfWithResults(searchFutures);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(@NonNull String segmentName, IteratorState continuationToken,
                                                                                Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException("keyIterator");
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(@NonNull String segmentName, IteratorState continuationToken,
                                                                                    Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException("entryIterator");
    }

    @Override
    public CompletableFuture<Void> registerListener(@NonNull UpdateListener listener, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException("registerListener");
    }

    @Override
    public boolean unregisterListener(@NonNull UpdateListener listener) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException("unregisterListener");
    }

    //endregion

    //region Helpers

    private CompletableFuture<EntryInfo> findEntry(DirectSegmentAccess segment, ArrayView key, long bucketOffset, TimeoutTimer timer) {
        final int maxReadLength = EntrySerializer.HEADER_LENGTH + EntrySerializer.MAX_KEY_LENGTH;
        AtomicLong offset = new AtomicLong(bucketOffset);
        // Read the Key at the current offset and check it against the sought one.
        CompletableFuture<EntryInfo> result = new CompletableFuture<>();
        Futures.loop(
                () -> !result.isDone(),
                () -> {
                    ReadResult readResult = segment.read(offset.get(), maxReadLength, timer.getRemaining());
                    KeyMatcher keyMatcher = new KeyMatcher(key, this.serializer, timer);
                    AsyncReadResultProcessor.process(readResult, keyMatcher, this.executor);
                    return keyMatcher.getResult()
                            .thenComposeAsync(header -> {
                                if (header == null) {
                                    // No match: Try to use backpointers to re-get offset and repeat.
                                    return this.keyIndex.getBackpointerOffset(segment, offset.get(), timer.getRemaining())
                                            .thenAccept(newOffset -> {
                                                offset.set(newOffset);
                                                if (newOffset < 0) {
                                                    // Could not find anything.
                                                    result.complete(null);
                                                }
                                            });
                                } else {
                                    // Match.
                                    result.complete(new EntryInfo(offset.get(), bucketOffset, header));
                                    return CompletableFuture.<Void>completedFuture(null);
                                }
                            }, this.executor);
                },
                this.executor)
                .exceptionally(ex -> {
                    result.completeExceptionally(ex);
                    return null;
                });
        return result;
    }

    private CompletableFuture<TableEntry> readEntry(DirectSegmentAccess segment, ArrayView key, EntryInfo entryInfo, TimeoutTimer timer) {
        if (entryInfo == null) {
            // Couldn't find anything.
            return CompletableFuture.completedFuture(null);
        } else {
            // Found it! Read Value from Segment and return.
            AsyncTableEntryBuilder builder = new AsyncTableEntryBuilder(key, entryInfo.header.getValueLength(), entryInfo.bucketOffset, timer);
            ReadResult readResult = segment.read(entryInfo.getValueSegmentOffset(), entryInfo.header.getValueLength(), timer.getRemaining());
            AsyncReadResultProcessor.process(readResult, builder, this.executor);
            return builder.getResult();
        }
    }

    //endregion

    //region EntryInfo

    @RequiredArgsConstructor
    private static class EntryInfo {
        final long offset;
        final long bucketOffset;
        final EntrySerializer.Header header;

        long getValueSegmentOffset() {
            return this.offset + this.header.getValueOffset();
        }
    }

    //endregion
}
