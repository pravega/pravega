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

import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Helps locate the appropriate {@link TableKey} or {@link TableEntry} in a {@link TableBucket}.
 *
 * @param <ResultT> Type of the objects returned by an instance of this class.
 */
@RequiredArgsConstructor
abstract class TableBucketReader<ResultT> {
    //region Members

    protected final EntrySerializer serializer = new EntrySerializer();
    private final DirectSegmentAccess segment;
    private final GetBackpointer getBackpointer;
    private final ScheduledExecutorService executor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link TableBucketReader} class that can read {@link TableEntry} instances.
     *
     * @param segment        A {@link DirectSegmentAccess} that can be used to read from the Segment.
     * @param getBackpointer A Function that, when invoked with a {@link DirectSegmentAccess} and an offset, will return
     *                       a Backpointer originating at that offset, or -1 if no such backpointer exists.
     * @param executor       An Executor for async operations.
     * @return A new instance of the {@link TableBucketReader} class.
     */
    static TableBucketReader<TableEntry> entry(@NonNull DirectSegmentAccess segment,
                                               @NonNull GetBackpointer getBackpointer, @NonNull ScheduledExecutorService executor) {
        return new TableBucketReader.Entry(segment, getBackpointer, executor);
    }

    /**
     * Creates a new instance of the {@link TableBucketReader} class that can read {@link TableKey} instances.
     *
     * @param segment        A {@link DirectSegmentAccess} that can be used to read from the Segment.
     * @param getBackpointer A Function that, when invoked with a {@link DirectSegmentAccess} and an offset, will return
     *                       a Backpointer originating at that offset, or -1 if no such backpointer exists.
     * @param executor       An Executor for async operations.
     * @return A new instance of the {@link TableBucketReader} class.
     */
    static TableBucketReader<TableKey> key(@NonNull DirectSegmentAccess segment,
                                           @NonNull GetBackpointer getBackpointer, @NonNull ScheduledExecutorService executor) {
        return new TableBucketReader.Key(segment, getBackpointer, executor);
    }

    //endregion

    //region Searching

    /**
     * Locates all {@link ResultT} instances in a TableBucket.
     *
     * @param bucketOffset The current segment offset of the Table Bucket we are looking into.
     * @param handler      A {@link Consumer} that will be invoked every time a {@link ResultT} is fetched. This will not
     *                     be invoked for any {@link ResultT} item that is marked as deleted.
     * @param timer        A {@link TimeoutTimer} for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation completed.
     */
    CompletableFuture<Void> findAll(long bucketOffset, Consumer<ResultT> handler, TimeoutTimer timer) {
        AtomicLong offset = new AtomicLong(bucketOffset);
        return Futures.loop(
                () -> offset.get() >= 0,
                () -> {
                    // Read the Key from the Segment.
                    ReadResult readResult = segment.read(offset.get(), getMaxReadLength(), timer.getRemaining());
                    val reader = getReader(null, offset.get(), timer);
                    AsyncReadResultProcessor.process(readResult, reader, this.executor);
                    return reader.getResult()
                            .thenComposeAsync(entryResult -> {
                                // Record the entry.
                                if (!isDeleted(entryResult)) {
                                    handler.accept(entryResult);
                                }

                                // Get the next Key Location for this bucket.
                                return this.getBackpointer.apply(segment, offset.get(), timer.getRemaining());
                            }, this.executor);
                },
                offset::set,
                this.executor);
    }

    /**
     * Locates all {@link ResultT} instances in a TableBucket.
     *
     * @param bucketOffset The current segment offset of the Table Bucket we are looking into.
     * @param timer        A {@link TimeoutTimer} for the operation.
     * @return A CompletableFuture that, when completed, will contain a List with the desired result items. This list
     * will exclude all {@link ResultT} items that are marked as deleted.
     */
    CompletableFuture<List<ResultT>> findAll(long bucketOffset, TimeoutTimer timer) {
        List<ResultT> result = new ArrayList<>();
        return findAll(bucketOffset, result::add, timer).thenApply(v -> result);
    }

    /**
     * Attempts to locate something in a TableBucket that matches a particular key.
     *
     * @param soughtKey    An {@link ArrayView} instance representing the Key we are looking for.
     * @param bucketOffset The current segment offset of the Table Bucket we are looking into.
     * @param timer        A {@link TimeoutTimer} for the operation.
     * @return A CompletableFuture that, when completed, will contain the desired result, or null of no such result
     * was found.
     */
    CompletableFuture<ResultT> find(ArrayView soughtKey, long bucketOffset, TimeoutTimer timer) {
        int maxReadLength = getMaxReadLength();

        // Read the Key at the current offset and check it against the sought one.
        AtomicLong offset = new AtomicLong(bucketOffset);
        CompletableFuture<ResultT> result = new CompletableFuture<>();
        Futures.loop(
                () -> !result.isDone(),
                () -> {
                    ReadResult readResult = this.segment.read(offset.get(), maxReadLength, timer.getRemaining());
                    val reader = getReader(soughtKey, offset.get(), timer);
                    AsyncReadResultProcessor.process(readResult, reader, this.executor);
                    return reader
                            .getResult()
                            .thenComposeAsync(r -> {
                                SearchContinuation sc = processResult(r, soughtKey);
                                if (sc == SearchContinuation.ResultFound) {
                                    result.complete(r);
                                } else if (sc == SearchContinuation.NoResult) {
                                    result.complete(null);
                                } else {
                                    return this.getBackpointer.apply(this.segment, offset.get(), timer.getRemaining())
                                            .thenAccept(newOffset -> {
                                                offset.set(newOffset);
                                                if (newOffset < 0) {
                                                    // Could not find anything.
                                                    result.complete(null);
                                                }
                                            });
                                }

                                return CompletableFuture.completedFuture(null);
                            });
                },
                this.executor)
                .exceptionally(ex -> {
                    result.completeExceptionally(ex);
                    return null;
                });
        return result;
    }

    /**
     * Gets a value indicating the maximum number of bytes that need to be read in order to fetch and process the result.
     */
    protected abstract int getMaxReadLength();

    /**
     * Creates a new instance of the {@link AsyncTableEntryReader} class.
     *
     * @param soughtKey     An {@link ArrayView} instance representing the Key we are looking for.
     * @param segmentOffset The offset within the Segment we are reading from.
     * @param timer         A {@link TimeoutTimer} for the operation.
     * @return An {@link AsyncTableEntryReader}.
     */
    protected abstract AsyncTableEntryReader<ResultT> getReader(ArrayView soughtKey, long segmentOffset, TimeoutTimer timer);

    /**
     * Processes the given result (which was obtained by the {@link AsyncTableEntryReader} provided by {@link #getReader}).
     *
     * @param result    The result to process.
     * @param soughtKey An {@link ArrayView} instance representing the Key we are looking for.
     * @return A {@link SearchContinuation} that indicates what is to be done next.
     */
    protected abstract SearchContinuation processResult(ResultT result, ArrayView soughtKey);

    protected abstract boolean isDeleted(ResultT resultT);

    //endregion

    //region Entry

    /**
     * {@link TableBucketReader} implementation that can read {@link TableEntry} instances.
     */
    private static class Entry extends TableBucketReader<TableEntry> {
        private Entry(DirectSegmentAccess segment, GetBackpointer getBackpointer, ScheduledExecutorService executor) {
            super(segment, getBackpointer, executor);
        }

        @Override
        protected int getMaxReadLength() {
            return EntrySerializer.MAX_SERIALIZATION_LENGTH;
        }

        @Override
        protected AsyncTableEntryReader<TableEntry> getReader(ArrayView soughtKey, long segmentOffset, TimeoutTimer timer) {
            return AsyncTableEntryReader.readEntry(soughtKey, segmentOffset, this.serializer, timer);
        }

        @Override
        protected SearchContinuation processResult(TableEntry entry, ArrayView soughtKey) {
            if (entry == null) {
                // No match: Continue searching if possible.
                return SearchContinuation.Continue;
            } else if (entry.getValue() == null) {
                // Key matched, but was a deletion.
                return SearchContinuation.NoResult;
            } else {
                // Match.
                return SearchContinuation.ResultFound;
            }
        }

        @Override
        protected boolean isDeleted(TableEntry tableEntry) {
            return tableEntry.getKey().getVersion() == TableKey.NOT_EXISTS;
        }
    }

    //endregion

    //region Key

    /**
     * {@link TableBucketReader} implementation that can read {@link TableKey} instances.
     */
    private static class Key extends TableBucketReader<TableKey> {
        private Key(DirectSegmentAccess segment, GetBackpointer getBackpointer, ScheduledExecutorService executor) {
            super(segment, getBackpointer, executor);
        }

        @Override
        protected int getMaxReadLength() {
            return EntrySerializer.HEADER_LENGTH + EntrySerializer.MAX_KEY_LENGTH;
        }

        @Override
        protected AsyncTableEntryReader<TableKey> getReader(ArrayView soughtKey, long segmentOffset, TimeoutTimer timer) {
            return AsyncTableEntryReader.readKey(segmentOffset, this.serializer, timer);
        }

        @Override
        protected SearchContinuation processResult(TableKey result, ArrayView soughtKey) {
            if (HashedArray.arrayEquals(soughtKey, result.getKey())) {
                // Match.
                return SearchContinuation.ResultFound;
            } else {
                // No match: Continue searching if possible.
                return SearchContinuation.Continue;
            }
        }

        @Override
        protected boolean isDeleted(TableKey tableKey) {
            return tableKey.getVersion() == TableKey.NOT_EXISTS;
        }
    }

    //endregion

    //region Helper Classes

    /**
     * Defines an action to be taken during a Search, after processing a particular result.
     */
    private enum SearchContinuation {
        /**
         * The current result is the one we are looking for, and the search can be completed with it.
         */
        ResultFound,
        /**
         * The current result is not the one we are looking for, but we may continue the search if possible.
         */
        Continue,
        /**
         * The current result is not the one we are looking for, and it is certain that we cannot find any other match
         * if we look further (so don't bother to).
         */
        NoResult
    }

    @FunctionalInterface
    interface GetBackpointer {
        CompletableFuture<Long> apply(DirectSegmentAccess segment, long offset, Duration timeout);
    }

    //endregion
}
