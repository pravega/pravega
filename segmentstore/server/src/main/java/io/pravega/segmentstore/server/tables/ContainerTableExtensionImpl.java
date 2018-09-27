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
import com.google.common.util.concurrent.Runnables;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.tables.IteratorState;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.UpdateListener;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentMetadata;
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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
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
        this.executor = executor;
        this.hasher = KeyHasher.sha512(HASH_CONFIG);
        this.keyIndex = new ContainerKeyIndex(segmentContainer.getId(), cacheFactory, cacheManager, this.executor);
        this.serializer = new EntrySerializer();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.keyIndex.close();
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

        return Collections.singletonList(new WriterTableProcessor(new TableWriterConnectorImpl(metadata), this.executor));
    }

    //endregion

    //region TableStore Implementation

    @Override
    public CompletableFuture<Void> createSegment(@NonNull String segmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.segmentContainer.createStreamSegment(segmentName,
                IndexWriter.generateInitialTableAttributes(),
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

        // Generate an Update Batch for all the entries (since we need to know their Key Hashes and relative offsets in
        // the batch itself).
        val updateBatch = batch(entries, TableEntry::getKey, this.serializer::getUpdateLength, TableKeyBatch.update());
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> this.keyIndex.update(segment, updateBatch,
                        () -> commit(entries, updateBatch.getLength(), this.serializer::serializeUpdate, segment, timer.getRemaining()), timer),
                        this.executor);
    }

    @Override
    public CompletableFuture<Void> remove(@NonNull String segmentName, @NonNull Collection<TableKey> keys, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Generate an Update Batch for all the keys (since we need to know their Key Hashes and relative offsets in
        // the batch itself).
        val removeBatch = batch(keys, key -> key, this.serializer::getRemovalLength, TableKeyBatch.removal());
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> this.keyIndex.update(segment, removeBatch,
                        () -> commit(keys, removeBatch.getLength(), this.serializer::serializeRemoval, segment, timer.getRemaining()), timer),
                        this.executor)
                .thenRun(Runnables.doNothing());
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

    private CompletableFuture<List<TableEntry>> get(DirectSegmentAccess segment, List<ArrayView> keys, List<Long> bucketOffsets, TimeoutTimer timer) {
        List<CompletableFuture<TableEntry>> searchFutures = new ArrayList<>();
        for (int i = 0; i < bucketOffsets.size(); i++) {
            long offset = bucketOffsets.get(i);
            if (offset == TableKey.NOT_EXISTS) {
                // Bucket does not exist, hence neither does the key.
                searchFutures.add(CompletableFuture.completedFuture(null));
            } else {
                // Find the sought entry in the segment, based on its key.
                ArrayView key = keys.get(i);
                searchFutures.add(findEntry(segment, key, offset, timer));
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

    private <T> TableKeyBatch batch(Collection<T> toBatch, Function<T, TableKey> getKey, Function<T, Integer> getLength, TableKeyBatch batch) {
        for (T item : toBatch) {
            val length = getLength.apply(item);
            val key = getKey.apply(item);
            batch.add(key, this.hasher.hash(key.getKey()), length);
        }

        Preconditions.checkArgument(batch.getLength() <= MAX_BATCH_SIZE,
                "Update Batch length (%s) exceeds the maximum limit.", MAX_BATCH_SIZE);
        return batch;
    }

    private <T> CompletableFuture<Long> commit(Collection<T> toCommit, int serializationLength, BiConsumer<Collection<T>, byte[]> serializer,
                                               DirectSegmentAccess segment, Duration timeout) {
        assert serializationLength <= MAX_BATCH_SIZE;
        byte[] s = new byte[serializationLength];
        serializer.accept(toCommit, s);
        return segment.append(s, null, timeout);
    }

    private CompletableFuture<TableEntry> findEntry(DirectSegmentAccess segment, ArrayView key, long bucketOffset, TimeoutTimer timer) {
        final int maxReadLength = EntrySerializer.HEADER_LENGTH + EntrySerializer.MAX_KEY_LENGTH;

        // Read the Key at the current offset and check it against the sought one.
        AtomicLong offset = new AtomicLong(bucketOffset);
        CompletableFuture<TableEntry> result = new CompletableFuture<>();
        Futures.loop(
                () -> !result.isDone(),
                () -> {
                    ReadResult readResult = segment.read(offset.get(), maxReadLength, timer.getRemaining());
                    val entryReader = AsyncTableEntryReader.readEntry(key, offset.get(), this.serializer, timer);
                    AsyncReadResultProcessor.process(readResult, entryReader, this.executor);
                    return entryReader
                            .getResult()
                            .thenComposeAsync(entry -> {
                                if (entry == null) {
                                    // No match: Try to use backpointers to re-get offset and repeat.
                                    return this.keyIndex
                                            .getBackpointerOffset(segment, offset.get(), timer.getRemaining())
                                            .thenAccept(newOffset -> {
                                                offset.set(newOffset);
                                                if (newOffset < 0) {
                                                    // Could not find anything.
                                                    result.complete(null);
                                                }
                                            });
                                } else {
                                    // Match.
                                    result.complete(entry);
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

    //endregion

    //region TableWriterConnector

    @RequiredArgsConstructor
    private class TableWriterConnectorImpl implements TableWriterConnector {
        @Getter
        private final SegmentMetadata metadata;

        @Override
        public EntrySerializer getSerializer() {
            return ContainerTableExtensionImpl.this.serializer;
        }

        @Override
        public KeyHasher getKeyHasher() {
            return ContainerTableExtensionImpl.this.hasher;
        }

        @Override
        public CompletableFuture<DirectSegmentAccess> getSegment(Duration timeout) {
            return ContainerTableExtensionImpl.this.segmentContainer.forSegment(this.metadata.getName(), timeout);
        }

        @Override
        public void notifyIndexOffsetChanged(long lastIndexedOffset) {
            // Not yet implemented. Will be done in issue 2878.
        }

        @Override
        public void close() {
            // Not yet implemented. Will be done in issue 2878.
        }
    }

    //endregion
}
