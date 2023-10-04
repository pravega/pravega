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
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.server.DirectSegmentAccess;

import java.io.IOException;
import java.time.Duration;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Iterates through a {@link DirectSegmentAccess}, deserializing {@link TableEntry} from a {@link BufferView} in linear fashion.
 *
 * @param <T> Type of the final, converted result.
 */
@Slf4j
@ThreadSafe
@Builder
class TableEntryDeltaIterator<T> implements AsyncIterator<T> {
    //region Members

    // The maximum size (in bytes) of each read to perform on the segment.
    private static final int MAX_READ_SIZE = 2 * 1024 * 1024;

    private final DirectSegmentAccess segment;
    // The offset to being iteration at.
    private final long startOffset;
    // Maximum length of the TableSegment we want to read until.
    private final int maxBytesToRead;
    private final boolean shouldClear;
    private final Duration fetchTimeout;
    private final EntrySerializer entrySerializer;
    private final ConvertResult<T> resultConverter;
    private final Executor executor;

    @GuardedBy("this")
    private Iterator<Map.Entry<DeltaIteratorState, TableEntry>> currentEntry;
    @GuardedBy("this")
    private long currentBatchOffset;

    //endregion

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

    public synchronized boolean endOfSegment() {
        return this.currentBatchOffset >= (this.startOffset + this.maxBytesToRead);
    }

    private synchronized CompletableFuture<Map.Entry<DeltaIteratorState, TableEntry>> getNextEntry() {
        val entry = getNextEntryFromBatch();
        if (entry != null) {
            return CompletableFuture.completedFuture(entry);
        }

        return fetchNextTableEntriesBatch().thenApply(val -> getNextEntryFromBatch());
    }

    private synchronized Map.Entry<DeltaIteratorState, TableEntry> getNextEntryFromBatch() {
        if (this.currentEntry != null) {
            val next = this.currentEntry.next();
            if (!this.currentEntry.hasNext()) {
                this.currentEntry = null;
            }
            return next;
        }

        return null;
    }

    private synchronized CompletableFuture<Void> fetchNextTableEntriesBatch() {
        return toEntries(currentBatchOffset)
                .thenAccept(entries -> {
                    if (!entries.isEmpty()) {
                        this.currentEntry = entries.iterator();
                    } else {
                        this.currentEntry = null;
                    }
                });
    }

    private CompletableFuture<List<Map.Entry<DeltaIteratorState, TableEntry>>> toEntries(long startOffset) {
        TimeoutTimer timer = new TimeoutTimer(this.fetchTimeout);
        int length = Math.min(maxBytesToRead, MAX_READ_SIZE);

        if (endOfSegment()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        ReadResult result = this.segment.read(startOffset, length, timer.getRemaining());
        return AsyncReadResultProcessor.processAll(result, this.executor, timer.getRemaining())
                .thenApply(data -> parseEntries(data, startOffset, length));
    }

    @SneakyThrows(IOException.class)
    private List<Map.Entry<DeltaIteratorState, TableEntry>> parseEntries(BufferView data, long startOffset, int readLength) {

        long currentOffset = startOffset;
        final long maxOffset = startOffset + readLength;

        BufferView.Reader input = data.getBufferViewReader();
        List<Map.Entry<DeltaIteratorState, TableEntry>> entries = new ArrayList<>();
        try {
            while (currentOffset < maxOffset) {
                val entry = AsyncTableEntryReader.readEntryComponents(input, currentOffset, this.entrySerializer);
                boolean reachedEnd = currentOffset + entry.getHeader().getTotalLength() >= this.maxBytesToRead + startOffset;
                // We must preserve deletions to accurately construct a delta.
                BufferView value = entry.getValue() == null ? BufferView.empty() : entry.getValue();
                currentOffset += entry.getHeader().getTotalLength();
                entries.add(new AbstractMap.SimpleEntry<>(
                        new DeltaIteratorState(currentOffset, reachedEnd, this.shouldClear, entry.getHeader().isDeletion()),
                        TableEntry.versioned(entry.getKey(), value, entry.getVersion())));
            }
        } catch (BufferView.Reader.OutOfBoundsException ex) {
            // Handles the event that our computed maxOffset lies within (but not on the boundary) of a TableEntry, or
            // reaches the end the TableSegment. Silently handling this exception is sufficient because it acknowledges
            // that we have processed the maximal set of TableEntries and thus is safe to return.
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
    @SuppressWarnings("ImportControl")
    static <T> TableEntryDeltaIterator<T> empty() {
        return new TableEntryDeltaIterator<>(
                null,
                0L,
                0,
                false,
                Duration.ofMillis(0),
                new EntrySerializer(),
                ignored -> CompletableFuture.completedFuture(null),
                java.util.concurrent.ForkJoinPool.commonPool(),
                null,
                0L);
    }

    //endregion

    @FunctionalInterface
    interface ConvertResult<T> {
        CompletableFuture<T> apply(Map.Entry<DeltaIteratorState, TableEntry> entry);
    }

    //endregion

}
