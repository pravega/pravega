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
package io.pravega.client.tables.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.KeyValueTableIterator;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * {@link KeyValueTableIterator} implementation.
 */
@RequiredArgsConstructor
class KeyValueTableIteratorImpl implements KeyValueTableIterator {
    //region Members

    @Getter
    @NonNull
    private final ByteBuffer fromPrimaryKey;
    @Getter
    @NonNull
    private final ByteBuffer fromSecondaryKey;
    @Getter
    @NonNull
    private final ByteBuffer toPrimaryKey;
    @Getter
    @NonNull
    private final ByteBuffer toSecondaryKey;
    private final int maxIterationSize;
    @NonNull
    private final TableEntryHelper entryConverter;
    @NonNull
    private final Executor executor;

    //endregion

    //region KeyValueTableIterator Implementation

    /**
     * Gets a value indicating whether it is guaranteed that this {@link KeyValueTableIterator} applies to a single
     * segment (partition) or not.
     *
     * @return True if guaranteed to be on a single segment, false otherwise.
     */
    boolean isSingleSegment() {
        return this.fromPrimaryKey.equals(this.toPrimaryKey);
    }

    @Override
    public AsyncIterator<IteratorItem<TableKey>> keys() {
        return isSingleSegment() ? singleSegmentKeys() : multiSegmentKeys();
    }

    @Override
    public AsyncIterator<IteratorItem<TableEntry>> entries() {
        return isSingleSegment() ? singleSegmentEntries() : multiSegmentEntries();
    }

    private AsyncIterator<IteratorItem<TableKey>> singleSegmentKeys() {
        assert this.fromPrimaryKey.equals(this.toPrimaryKey);
        return singleSegmentKeys(this.entryConverter.getSelector().getTableSegment(this.fromPrimaryKey));
    }

    private AsyncIterator<IteratorItem<TableKey>> singleSegmentKeys(TableSegment ts) {
        return singleSegmentIterator(ts.keyIterator(getIteratorArgs()), this.entryConverter::fromTableSegmentKey);
    }

    private AsyncIterator<IteratorItem<TableEntry>> singleSegmentEntries() {
        assert this.fromPrimaryKey.equals(this.toPrimaryKey);
        return singleSegmentEntries(this.entryConverter.getSelector().getTableSegment(this.fromPrimaryKey));
    }

    private AsyncIterator<IteratorItem<TableEntry>> singleSegmentEntries(TableSegment ts) {
        return singleSegmentIterator(ts.entryIterator(getIteratorArgs()), e -> this.entryConverter.fromTableSegmentEntry(ts, e));
    }

    private <T, V> AsyncIterator<IteratorItem<V>> singleSegmentIterator(AsyncIterator<IteratorItem<T>> tsIterator, Function<T, V> convert) {
        return tsIterator
                .thenApply(si -> {
                    val entries = si.getItems().stream().map(convert).collect(Collectors.toList());
                    return new IteratorItem<>(entries);
                });
    }

    private AsyncIterator<IteratorItem<TableKey>> multiSegmentKeys() {
        return multiSegment(this::singleSegmentKeys, k -> k);
    }

    private AsyncIterator<IteratorItem<TableEntry>> multiSegmentEntries() {
        return multiSegment(this::singleSegmentEntries, TableEntry::getKey);
    }

    private <T> AsyncIterator<IteratorItem<T>> multiSegment(Function<TableSegment, AsyncIterator<IteratorItem<T>>> segmentIterator,
                                                            Function<T, TableKey> getKey) {
        // Issue single-segment iterators to each segment and flatten them.
        val segmentIterators = this.entryConverter.getSelector().getAllTableSegments()
                .stream()
                .map(segmentIterator)
                .iterator();

        // Return a MergeAsyncIterator with all of them.
        return new MergeAsyncIterator<>(segmentIterators, getKey, this.maxIterationSize, this.executor)
                .asSequential(this.executor); // Ensure that we won't get overlapping requests from the user.
    }

    private SegmentIteratorArgs getIteratorArgs() {
        return SegmentIteratorArgs.builder()
                .maxItemsAtOnce(this.maxIterationSize)
                .fromKey(this.entryConverter.serializeKey(this.fromPrimaryKey, this.fromSecondaryKey))
                .toKey(this.entryConverter.serializeKey(this.toPrimaryKey, this.toSecondaryKey))
                .build();
    }

    //endregion

    //region MergeAsyncIterator

    /**
     * A Merge iterator of individual Segment Iterators.
     * <p>
     * General algorithm:
     * 1. Initiate single-segment iterators (this is our input).
     * 2. Flatten them (one item at a time).
     * 3. Fetch the initial batch from each iterators, exclude those with no data, and sort by TableKey (lowest to highest).
     * 4. With each call to {@link #getNext()}, retrieve first item from lowest-ordered iterator. Repeat until we fill our batch,
     * making sure to always keep the individual iterators sorted by TableKey (every retrieval may affect the order).
     * 5. When a single-segment iterator is done, remove from list of iterators.
     * 6. Complete when there are no more segment iterators to iterate on.
     *
     * @param <T> Resulting item type.
     */
    @VisibleForTesting
    static class MergeAsyncIterator<T> implements AsyncIterator<IteratorItem<T>> {
        private static final TableKeyComparator COMPARATOR = new TableKeyComparator();
        private final CompletableFuture<PriorityQueue<PeekingIterator<T>>> segments;
        private final Function<T, TableKey> getKey;
        private final int maxIterationSize;
        private final Executor executor;

        MergeAsyncIterator(Iterator<AsyncIterator<IteratorItem<T>>> iterators, Function<T, TableKey> getKey,
                           int maxIterationSize, Executor executor) {
            this.getKey = getKey;
            this.maxIterationSize = maxIterationSize;
            this.segments = initialize(iterators);
            this.executor = executor;
        }

        @Override
        public CompletableFuture<IteratorItem<T>> getNext() {
            return this.segments
                    .thenCompose(segments -> {
                        if (segments.isEmpty()) {
                            // Iterators are removed as they are done; if we have none left, we are all done.
                            return CompletableFuture.completedFuture(null);
                        }

                        return populateBatch(segments);
                    });
        }

        private CompletableFuture<IteratorItem<T>> populateBatch(PriorityQueue<PeekingIterator<T>> segments) {
            val result = new ArrayList<T>();
            return Futures.loop(
                    () -> !segments.isEmpty() && result.size() < this.maxIterationSize,
                    () -> {
                        val first = segments.poll();
                        result.add(first.getCurrent().getItem());
                        return first.advance()
                                .thenRun(() -> {
                                    if (first.hasNext()) {
                                        // There are still items in the segment's iterator; re-add it to the heap to add to proper position.
                                        segments.add(first);
                                    }
                                });
                    },
                    this.executor)
                    .thenApply(v -> new IteratorItem<>(result));
        }

        private CompletableFuture<PriorityQueue<PeekingIterator<T>>> initialize(Iterator<AsyncIterator<IteratorItem<T>>> iterators) {
            // Fetch the first item(s) in each iterator and sort them by their first Table Key.
            val moveFirst = new HashMap<PeekingIterator<T>, CompletableFuture<Void>>();
            while (iterators.hasNext()) {
                val ss = new PeekingIterator<>(iterators.next(), this.getKey);
                moveFirst.put(ss, ss.advance());
            }

            val result = new PriorityQueue<PeekingIterator<T>>((s1, s2) -> COMPARATOR.compare(s1.getCurrent().getKey(), s2.getCurrent().getKey()));
            return Futures.allOf(moveFirst.values())
                    .thenApply(v -> {
                        // Clear out those iterators with no values, and add the rest to the heap.
                        moveFirst.keySet().stream().filter(PeekingIterator::hasNext).forEach(result::add);
                        return result;
                    });
        }
    }

    //endregion

    //region FlattenedAsyncIterator

    /**
     * Flattens {@link AsyncIterator} instances with a List of items into an iterator-like that returns one item at a time.
     *
     * @param <T> Item type.
     */
    @RequiredArgsConstructor
    @VisibleForTesting
    static class PeekingIterator<T> {
        private final AsyncIterator<IteratorItem<T>> innerIterator;
        private final Function<T, TableKey> getKey;
        private final AtomicReference<List<PeekingIteratorItem<T>>> currentBatch = new AtomicReference<>();
        private final AtomicInteger currentBatchIndex = new AtomicInteger();

        PeekingIteratorItem<T> getCurrent() {
            return this.currentBatch.get() == null ? null : this.currentBatch.get().get(this.currentBatchIndex.get());
        }

        boolean hasNext() {
            return this.currentBatch.get() != null;
        }

        CompletableFuture<Void> advance() {
            // See if we have anything in our buffer.
            if (this.currentBatch.get() != null && this.currentBatchIndex.get() < this.currentBatch.get().size() - 1) {
                this.currentBatchIndex.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }

            // Completely consumed our buffer. Request next batch.
            return this.innerIterator.getNext()
                    .thenAccept(result -> {
                        // Null result means the inner iterator is done - so we must follow suit.
                        if (result == null || result.getItems().isEmpty()) {
                            this.currentBatch.set(null);
                        } else {
                            this.currentBatch.set(result.getItems().stream()
                                    .map(i -> new PeekingIteratorItem<>(i, this.getKey.apply(i)))
                                    .collect(Collectors.toList()));
                        }
                        this.currentBatchIndex.set(0);
                    });
        }
    }

    @Data
    @VisibleForTesting
    static class PeekingIteratorItem<T> {
        final T item;
        final TableKey key;
    }

    //endregion

    //region TableKeyComparator

    /**
     * {@link TableKey} and {@link ByteBuffer} comparator. Uses unsigned byte comparison to simulate bitwise lexicographical
     * order.
     */
    @VisibleForTesting
    static class TableKeyComparator implements Comparator<TableKey> {
        @Override
        public int compare(TableKey k1, TableKey k2) {
            int r = compare(k1.getPrimaryKey(), k2.getPrimaryKey());
            if (r == 0 && k1.getSecondaryKey() != null) {
                r = compare(k1.getSecondaryKey(), k2.getSecondaryKey());
            }
            return r;
        }

        int compare(ByteBuffer b1, ByteBuffer b2) {
            assert b1.remaining() == b2.remaining();
            int r;
            for (int i = 0; i < b1.remaining(); i++) {
                // Unsigned comparison mimics bitwise comparison.
                r = (b1.get(i) & 0xFF) - (b2.get(i) & 0xFF);
                if (r != 0) {
                    return r;
                }
            }

            return 0;
        }
    }

    //endregion

    //region Builder

    /**
     * Builder for {@link KeyValueTableIteratorImpl} instances.
     */
    @RequiredArgsConstructor
    static class Builder implements KeyValueTableIterator.Builder {
        @VisibleForTesting
        static final byte MIN_BYTE = (byte) 0;
        @VisibleForTesting
        static final byte MAX_BYTE = (byte) 0xFF;
        @NonNull
        private final KeyValueTableConfiguration kvtConfig;
        @NonNull
        private final TableEntryHelper entryConverter;
        @NonNull
        private final Executor executor;
        private int maxIterationSize = 10;

        @Override
        public KeyValueTableIterator.Builder maxIterationSize(int size) {
            Preconditions.checkArgument(size > 0, "size must be a positive integer");
            this.maxIterationSize = size;
            return this;
        }

        @Override
        public KeyValueTableIteratorImpl forPrimaryKey(@NonNull ByteBuffer primaryKey) {
            return forPrimaryKey(primaryKey, null, null);
        }

        @Override
        public KeyValueTableIteratorImpl forPrimaryKey(@NonNull ByteBuffer primaryKey, ByteBuffer fromSecondaryKey, ByteBuffer toSecondaryKey) {
            validateExact(primaryKey, this.kvtConfig.getPrimaryKeyLength(), "Primary Key");
            validateExact(fromSecondaryKey, this.kvtConfig.getSecondaryKeyLength(), "From Secondary Key");
            validateExact(toSecondaryKey, this.kvtConfig.getSecondaryKeyLength(), "To Secondary Key");

            // If these are null, pad() will replace them with appropriately sized buffers.
            fromSecondaryKey = pad(fromSecondaryKey, MIN_BYTE, this.kvtConfig.getSecondaryKeyLength());
            toSecondaryKey = pad(toSecondaryKey, MAX_BYTE, this.kvtConfig.getSecondaryKeyLength());
            return new KeyValueTableIteratorImpl(primaryKey, fromSecondaryKey, primaryKey, toSecondaryKey,
                    this.maxIterationSize, this.entryConverter, this.executor);
        }

        @Override
        public KeyValueTableIteratorImpl forPrimaryKey(@NonNull ByteBuffer primaryKey, ByteBuffer secondaryKeyPrefix) {
            validateExact(primaryKey, this.kvtConfig.getPrimaryKeyLength(), "Primary Key");
            validateAtMost(secondaryKeyPrefix, this.kvtConfig.getSecondaryKeyLength(), "Secondary Key Prefix");

            // If secondaryKeyPrefix is null, pad() will replace it with the effective Min/Max values, as needed.
            val fromSecondaryKey = pad(secondaryKeyPrefix, (byte) 0, this.kvtConfig.getSecondaryKeyLength());
            val toSecondaryKey = pad(secondaryKeyPrefix, (byte) 0xFF, this.kvtConfig.getSecondaryKeyLength());
            return new KeyValueTableIteratorImpl(primaryKey, fromSecondaryKey, primaryKey, toSecondaryKey,
                    this.maxIterationSize, this.entryConverter, this.executor);
        }

        @Override
        public KeyValueTableIteratorImpl forRange(ByteBuffer fromPrimaryKey, ByteBuffer toPrimaryKey) {
            validateExact(fromPrimaryKey, this.kvtConfig.getPrimaryKeyLength(), "From Primary Key");
            validateExact(toPrimaryKey, this.kvtConfig.getPrimaryKeyLength(), "To Primary Key");

            // If these are null, pad() will replace them with appropriately sized buffers.
            fromPrimaryKey = pad(fromPrimaryKey, MIN_BYTE, this.kvtConfig.getPrimaryKeyLength());
            toPrimaryKey = pad(toPrimaryKey, MAX_BYTE, this.kvtConfig.getPrimaryKeyLength());

            // SecondaryKeys must be the full range in this case, otherwise the resulting iterator will have non-contiguous ranges.
            val fromSecondaryKey = pad(null, MIN_BYTE, this.kvtConfig.getSecondaryKeyLength());
            val toSecondaryKey = pad(null, MAX_BYTE, this.kvtConfig.getSecondaryKeyLength());
            return new KeyValueTableIteratorImpl(fromPrimaryKey, fromSecondaryKey, toPrimaryKey, toSecondaryKey,
                    this.maxIterationSize, this.entryConverter, this.executor);
        }

        @Override
        public KeyValueTableIteratorImpl forPrefix(ByteBuffer primaryKeyPrefix) {
            validateAtMost(primaryKeyPrefix, this.kvtConfig.getPrimaryKeyLength(), "Primary Key Prefix");

            val fromPrimaryKey = pad(primaryKeyPrefix, MIN_BYTE, this.kvtConfig.getPrimaryKeyLength());
            val toPrimaryKey = pad(primaryKeyPrefix, MAX_BYTE, this.kvtConfig.getPrimaryKeyLength());

            // SecondaryKeys must be the full range in this case, otherwise the resulting iterator will have non-contiguous ranges.
            val fromSecondaryKey = pad(null, MIN_BYTE, this.kvtConfig.getSecondaryKeyLength());
            val toSecondaryKey = pad(null, MAX_BYTE, this.kvtConfig.getSecondaryKeyLength());
            return new KeyValueTableIteratorImpl(fromPrimaryKey, fromSecondaryKey, toPrimaryKey, toSecondaryKey,
                    this.maxIterationSize, this.entryConverter, this.executor);
        }

        @Override
        public KeyValueTableIteratorImpl all() {
            return forRange(null, null);
        }

        private ByteBuffer pad(ByteBuffer key, byte value, int size) {
            byte[] result;
            int startOffset;
            if (key == null) {
                result = new byte[size];
                startOffset = 0;
            } else {
                if (key.remaining() == size) {
                    // Already at the right size.
                    return key.duplicate();
                }
                result = new byte[size];
                startOffset = key.remaining();
                key.duplicate().get(result, 0, startOffset);
            }

            for (int i = startOffset; i < size; i++) {
                result[i] = value;
            }
            return ByteBuffer.wrap(result);
        }

        private void validateExact(@Nullable ByteBuffer key, int expected, String name) {
            if (key != null) {
                Preconditions.checkArgument(expected == key.remaining(), "%s length must be %s; given %s.", name, expected, key.remaining());
            }
        }

        private void validateAtMost(ByteBuffer key, int maxLength, String name) {
            if (key != null) {
                Preconditions.checkArgument(key.remaining() <= maxLength, "%s length must be at most %s; given %s.", name, maxLength, key.remaining());
            }
        }
    }

    //endregion
}

