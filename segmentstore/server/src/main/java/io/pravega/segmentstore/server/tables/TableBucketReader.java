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

import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
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
    private final Executor executor;

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
                                               @NonNull GetBackpointer getBackpointer, @NonNull Executor executor) {
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
                                           @NonNull GetBackpointer getBackpointer, @NonNull Executor executor) {
        return new TableBucketReader.Key(segment, getBackpointer, executor);
    }

    //endregion

    //region Searching

    /**
     * Locates all {@link ResultT} instances in a TableBucket.
     *
     * @param bucketOffset The current segment offset of the Table Bucket we are looking into.
     * @param timer        A {@link TimeoutTimer} for the operation.
     * @return A CompletableFuture that, when completed, will contain a List with the desired result items. This list
     * will exclude all {@link ResultT} items that are marked as deleted.
     */
    CompletableFuture<List<ResultT>> findAllExisting(long bucketOffset, TimeoutTimer timer) {
        val result = new HashMap<BufferView, ResultT>();

        // This handler ensures that items are only added once (per key) and only if they are not deleted. Since the items
        // are processed in descending version order, the first time we encounter its key is its latest value.
        BiConsumer<ResultT, Long> handler = (item, offset) -> {
            TableKey key = getKey(item);
            if (!result.containsKey(key.getKey())) {
                result.put(key.getKey(), key.getVersion() == TableKey.NOT_EXISTS ? null : item);
            }
        };
        return findAll(bucketOffset, handler, timer)
                .thenApply(v -> result.values().stream().filter(Objects::nonNull).collect(Collectors.toList()));
    }

    /**
     * Locates all {@link ResultT} instances in a TableBucket.
     *
     * @param bucketOffset The current segment offset of the Table Bucket we are looking into.
     * @param handler      A {@link BiConsumer} that will be invoked every time a {@link ResultT} is fetched. This will not
     *                     be invoked for any {@link ResultT} item that is marked as deleted. The second argument indicates
     *                     the Segment Offset where the given item resides.
     * @param timer        A {@link TimeoutTimer} for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation completed.
     */
    CompletableFuture<Void> findAll(long bucketOffset, BiConsumer<ResultT, Long> handler, TimeoutTimer timer) {
        AtomicLong offset = new AtomicLong(bucketOffset);
        return Futures.loop(
                () -> offset.get() >= 0,
                () -> {
                    // Read the Key from the Segment. Copy it out of the Segment to avoid losing it or getting corrupted
                    // values back in case of a cache eviction. See {@link ReadResult#setCopyOnRead(boolean)}.
                    ReadResult readResult = segment.read(offset.get(), getMaxReadLength(), timer.getRemaining());
                    val reader = getReader(null, offset.get(), timer);
                    AsyncReadResultProcessor.process(readResult, reader, this.executor);
                    return reader.getResult()
                            .thenComposeAsync(entryResult -> {
                                // Record the entry, but only if we haven't processed its Key before and only if it exists.
                                handler.accept(entryResult, offset.get());

                                // Get the next Key Location for this bucket.
                                return this.getBackpointer.apply(segment, offset.get(), timer.getRemaining());
                            }, this.executor);
                },
                offset::set,
                this.executor);
    }

    /**
     * Attempts to locate something in a TableBucket that matches a particular key.
     *
     * @param soughtKey    A {@link BufferView} instance representing the Key we are looking for.
     * @param bucketOffset The current segment offset of the Table Bucket we are looking into.
     * @param timer        A {@link TimeoutTimer} for the operation.
     * @return A CompletableFuture that, when completed, will contain the desired result, or null of no such result
     * was found.
     */
    CompletableFuture<ResultT> find(BufferView soughtKey, long bucketOffset, TimeoutTimer timer) {
        int maxReadLength = getMaxReadLength();

        // Read the Key at the current offset and check it against the sought one.
        AtomicLong offset = new AtomicLong(bucketOffset);
        CompletableFuture<ResultT> result = new CompletableFuture<>();
        Futures.loop(
                () -> !result.isDone(),
                () -> {
                    // Read the Key from the Segment. Copy it out of the Segment to avoid losing it or getting corrupted
                    // values back in case of a cache eviction. See {@link ReadResult#setCopyOnRead(boolean)}.
                    ReadResult readResult = this.segment.read(offset.get(), maxReadLength, timer.getRemaining());
                    val reader = getReader(soughtKey, offset.get(), timer);
                    AsyncReadResultProcessor.process(readResult, reader, this.executor);
                    return reader
                            .getResult()
                            .thenComposeAsync(r -> {
                                SearchContinuation sc = processResult(r, soughtKey);
                                if (sc == SearchContinuation.ResultFound || sc == SearchContinuation.NoResult) {
                                    // We either definitely found the result or definitely did not find the result.
                                    // In the case we did not find what we were looking for, we may still have some
                                    // partial result to return to the caller (i.e., a TableEntry with no value, but with
                                    // a version, which indicates a deleted entry (as opposed from an inexistent one).
                                    result.complete(r);
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
                            }, this.executor);
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
     * @param soughtKey     A {@link BufferView} instance representing the Key we are looking for.
     * @param segmentOffset The offset within the Segment we are reading from.
     * @param timer         A {@link TimeoutTimer} for the operation.
     * @return An {@link AsyncTableEntryReader}.
     */
    protected abstract AsyncTableEntryReader<ResultT> getReader(BufferView soughtKey, long segmentOffset, TimeoutTimer timer);

    /**
     * Processes the given result (which was obtained by the {@link AsyncTableEntryReader} provided by {@link #getReader}).
     *
     * @param result    The result to process.
     * @param soughtKey A {@link BufferView} instance representing the Key we are looking for.
     * @return A {@link SearchContinuation} that indicates what is to be done next.
     */
    protected abstract SearchContinuation processResult(ResultT result, BufferView soughtKey);

    protected abstract TableKey getKey(ResultT resultT);

    //endregion

    //region Entry

    /**
     * {@link TableBucketReader} implementation that can read {@link TableEntry} instances.
     */
    private static class Entry extends TableBucketReader<TableEntry> {
        private Entry(DirectSegmentAccess segment, GetBackpointer getBackpointer, Executor executor) {
            super(segment, getBackpointer, executor);
        }

        @Override
        protected int getMaxReadLength() {
            return EntrySerializer.MAX_SERIALIZATION_LENGTH;
        }

        @Override
        protected AsyncTableEntryReader<TableEntry> getReader(BufferView soughtKey, long segmentOffset, TimeoutTimer timer) {
            return AsyncTableEntryReader.readEntry(soughtKey, segmentOffset, this.serializer, timer);
        }

        @Override
        protected SearchContinuation processResult(TableEntry entry, BufferView soughtKey) {
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
        protected TableKey getKey(TableEntry tableEntry) {
            return tableEntry.getKey();
        }
    }

    //endregion

    //region Key

    /**
     * {@link TableBucketReader} implementation that can read {@link TableKey} instances.
     */
    private static class Key extends TableBucketReader<TableKey> {
        private Key(DirectSegmentAccess segment, GetBackpointer getBackpointer, Executor executor) {
            super(segment, getBackpointer, executor);
        }

        @Override
        protected int getMaxReadLength() {
            return EntrySerializer.HEADER_LENGTH + EntrySerializer.MAX_KEY_LENGTH;
        }

        @Override
        protected AsyncTableEntryReader<TableKey> getReader(BufferView soughtKey, long segmentOffset, TimeoutTimer timer) {
            return AsyncTableEntryReader.readKey(segmentOffset, this.serializer, timer);
        }

        @Override
        protected SearchContinuation processResult(TableKey result, BufferView soughtKey) {
            if (soughtKey.equals(result.getKey())) {
                // Match.
                return SearchContinuation.ResultFound;
            } else {
                // No match: Continue searching if possible.
                return SearchContinuation.Continue;
            }
        }

        @Override
        protected TableKey getKey(TableKey tableKey) {
            return tableKey;
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
