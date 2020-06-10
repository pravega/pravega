/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.server.DirectSegmentAccess;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Iterates through {@link TableBucket}s in a Segment.
 * @param <T> Type of the final, converted result.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@ThreadSafe
class TableEntryIterator<T> implements AsyncIterator<T> {
    //region Members

    // The maximum size (in bytes) of each read to perform on the segment.
    private static final int MAX_READ_SIZE = 2 * 1024 * 1024;

    private final DirectSegmentAccess segment;
    private final long startOffset;
    private final int maxLength;
    private final boolean shouldClear;
    private final Duration fetchTimeout;
    private final EntrySerializer entrySerializer;
    private final ConvertResult<T> resultConverter;
    private final Executor executor;
    @GuardedBy("this")
    private Iterator<Map.Entry<EntryIteratorState, TableEntry>> currentEntry = null;
    @GuardedBy("this")
    private long currentBatchOffset;
    //endregion

    private TableEntryIterator<T> setCurrentBatchOffset(long currentBatchOffset) {
        this.currentBatchOffset = currentBatchOffset;
        return this;
    }

    //region AsyncIterator Implementation

    @Override
    public CompletableFuture<T> getNext() {
        // Verify no other call to getNext() is currently executing.
        return getNextEntry()
                .thenCompose(entry -> {
                    if (entry == null) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return this.resultConverter.apply(entry);
                    }
                });
    }

    public boolean endOfSegment() {
        return this.currentBatchOffset >= (startOffset + maxLength);
    }

    private CompletableFuture<Map.Entry<EntryIteratorState, TableEntry>> getNextEntry() {
        val entry = getNextEntryFromBatch();
        if (entry != null) {
            return CompletableFuture.completedFuture(entry);
        }

        return fetchNextTableEntriesBatch().thenApply(val -> getNextEntryFromBatch());
    }

    private synchronized Map.Entry<EntryIteratorState, TableEntry> getNextEntryFromBatch() {
        if (this.currentEntry != null) {
            Map.Entry<EntryIteratorState, TableEntry> next = this.currentEntry.next();
            if (!this.currentEntry.hasNext()) {
                this.currentEntry = null;
            }
            return next;
        }

        return null;
    }

    private CompletableFuture<Void> fetchNextTableEntriesBatch() {
        return toEntries(currentBatchOffset)
                .thenAccept(entries -> {
                    if (!entries.isEmpty()) {
                        this.currentEntry = entries.iterator();
                    } else {
                        this.currentEntry = null;
                    }
                });
    }

    private CompletableFuture<List<Map.Entry<EntryIteratorState, TableEntry>>> toEntries(long startOffset) {
        TimeoutTimer timer = new TimeoutTimer(this.fetchTimeout);
        int length = Math.min(maxLength, MAX_READ_SIZE);

        if (endOfSegment()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        ReadResult result = this.segment.read(startOffset, length, timer.getRemaining());
        return AsyncReadResultProcessor.processAll(result, this.executor, timer.getRemaining())
                .thenApply(data -> parseEntries(data, startOffset, length));
    }

    @SneakyThrows(IOException.class)
    private List<Map.Entry<EntryIteratorState, TableEntry>> parseEntries(BufferView data, long startOffset, int readLength) {

        long maxOffset = startOffset + readLength;
        long currentOffset = startOffset;

        InputStream input = data.getReader();
        List<Map.Entry<EntryIteratorState, TableEntry>> entries = new ArrayList<>();
        try {
            while (currentOffset < maxOffset) {
                val entry = AsyncTableEntryReader.readEntryComponents(input, currentOffset, this.entrySerializer);
                boolean reachedEnd = currentOffset + entry.getHeader().getTotalLength() >= maxLength + startOffset;
                // We must preserve deletions to accurately construct a delta.
                byte[] value = entry.getValue() == null ? new byte[0] : entry.getValue();
                entries.add(new AbstractMap.SimpleEntry<>(
                        new EntryIteratorState(currentOffset, reachedEnd, shouldClear, entry.getHeader().isDeletion()),
                        TableEntry.versioned(new ByteArraySegment(entry.getKey()), new ByteArraySegment(value), entry.getVersion())));
                currentOffset += entry.getHeader().getTotalLength();
            }

        } catch (EOFException ex) {
            input.close();
        }
        this.currentBatchOffset = currentOffset;

        return entries;
    }

    /**
     * Creates a new {@link TableIterator} that contains no elements.
     *
     * @param <T> Type of elements returned at each iteration.
     * @return A new instance of the {@link TableIterator.Builder} class.
     */
    static <T> TableEntryIterator<T> empty() {
        return new TableEntryIterator<>(
                null,
                0L,
                0,
                false,
                Duration.ofMillis(0),
                new EntrySerializer(),
                ignored -> CompletableFuture.completedFuture(null),
                ForkJoinPool.commonPool());
    }

    //endregion

    //region Builder

    /**
     * Creates a new {@link TableIterator.Builder} that can be used to construct {@link TableIterator} instances.
     *
     * @param <T> Type of the elements returned at each iteration.
     * @return A new instance of the {@link TableIterator.Builder} class.
     */
    static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Builder for the {@link TableIterator} class.
     */
    static class Builder<T> {

        private int maxLength = MAX_READ_SIZE;
        private long startOffset = 0;
        private boolean shouldClear = false;
        private DirectSegmentAccess segment;
        private ScheduledExecutorService executor;
        private EntrySerializer entrySerializer;
        private Duration fetchTimeout;
        private ConvertResult<T> resultConverter;

        /**
         * Sets a {@link DirectSegmentAccess} representing a Table Segment thjjat the iterator will iterate over.
         *
         * @param segment The {@link DirectSegmentAccess} to associate.
         * @return This object.
         */
        Builder<T> segment(@NonNull DirectSegmentAccess segment) {
            this.segment = segment;
            return this;
        }

        /**
         * Sets the maximum number of bytes to iterate over.
         *
         * @param maxLength
         * @return
         */
        Builder<T> maxLength(int maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        /**
         * Sets the offset for which to start iterating from.
         *
         * @param startOffset
         * @return
         */
        Builder<T> startOffset(long startOffset) {
            this.startOffset = startOffset;
            return this;
        }

        /**
         * Sets whether or not the iterator will set the clear flag (indicating the position of the key provided has
         * been truncated).
         *
         * @param shouldClear
         * @return
         */
        Builder<T> shouldClear(boolean shouldClear) {
            this.shouldClear = shouldClear;
            return this;
        }

        /**
         * Sets the Executor to use for async operations.
         *
         * @param executor The Executor to set.
         * @return This object.
         */
        Builder<T> executor(@NonNull ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }


        /**
         * Sets a {@link TableIterator.ConvertResult} function that will translate each {@link TableBucket} instance into the desired
         * final result.
         *
         * @param resultConverter A Function that will translate each {@link TableBucket} instance into the desired
         *                        final result.
         * @return This object.
         */
        Builder<T> resultConverter(@NonNull ConvertResult<T> resultConverter) {
            this.resultConverter = resultConverter;
            return this;
        }

        /**
         * Sets a Duration representing the Timeout for each invocation to {@link TableIterator#getNext()}.
         *
         * @param fetchTimeout Timeout to set.
         * @return This object.
         */
        Builder<T> fetchTimeout(@NonNull Duration fetchTimeout) {
            this.fetchTimeout = fetchTimeout;
            return this;
        }

        Builder<T> entrySerializer(@NonNull EntrySerializer entrySerializer) {
            this.entrySerializer = entrySerializer;
            return this;
        }

        /**
         * Creates a new instance of the {@link TableIterator} class using the information collected in this Builder.
         *
         * @return A CompletableFuture that, when completed, will contain the desired {@link TableIterator} instance.
         */
        CompletableFuture<AsyncIterator<T>> build() {
            return CompletableFuture.completedFuture(new TableEntryIterator<>(
                    this.segment,
                    this.startOffset,
                    this.maxLength,
                    this.shouldClear,
                    this.fetchTimeout,
                    this.entrySerializer,
                    this.resultConverter,
                    this.executor)
                    .setCurrentBatchOffset(this.startOffset));
        }

    }

    @FunctionalInterface
    interface ConvertResult<T> {
        CompletableFuture<T> apply(Map.Entry<EntryIteratorState, TableEntry> entry);
    }

    //endregion

}
