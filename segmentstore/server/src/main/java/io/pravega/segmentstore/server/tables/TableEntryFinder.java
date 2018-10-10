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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
class TableEntryFinder {
    @NonNull
    private final ContainerKeyIndex keyIndex;
    @NonNull
    private final EntrySerializer serializer;
    @NonNull
    private final ScheduledExecutorService executor;

    CompletableFuture<TableKey> findKey(@NonNull DirectSegmentAccess segment, @NonNull ArrayView soughtKey, long bucketOffset, TimeoutTimer timer) {
        final int maxReadLength = EntrySerializer.MAX_SERIALIZATION_LENGTH;

        // Read the Key at the current offset and check it against the sought one.
        AtomicLong offset = new AtomicLong(bucketOffset);
        CompletableFuture<TableKey> result = new CompletableFuture<>();

        val key = new HashedArray(soughtKey);
        Futures.loop(
                () -> !result.isDone(),
                () -> {
                    ReadResult readResult = segment.read(offset.get(), maxReadLength, timer.getRemaining());
                    val entryReader = AsyncTableEntryReader.readKey(this.serializer, timer);
                    AsyncReadResultProcessor.process(readResult, entryReader, this.executor);
                    return entryReader
                            .getResult()
                            .thenComposeAsync(keyData -> {
                                if (key.equals(new HashedArray(keyData))) {
                                    // Match.
                                    result.complete(TableKey.versioned(keyData, offset.get()));
                                    return CompletableFuture.<Void>completedFuture(null);
                                } else {
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

    CompletableFuture<TableEntry> findEntry(@NonNull DirectSegmentAccess segment, @NonNull ArrayView key, long bucketOffset, TimeoutTimer timer) {
        final int maxReadLength = EntrySerializer.MAX_SERIALIZATION_LENGTH;

        // Read the Key at the current offset and check it against the sought one.
        AtomicLong offset = new AtomicLong(bucketOffset);
        CompletableFuture<TableEntry> result = new CompletableFuture<>();
        Futures.loop(
                () -> !result.isDone(),
                () -> {
                    ReadResult readResult = segment.read(offset.get(), maxReadLength, timer.getRemaining());
                    val entryReader = AsyncTableEntryReader.readEntry(key, bucketOffset, this.serializer, timer);
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
                                } else if (entry.getValue() == null) {
                                    // Key matched, but was a deletion.
                                    result.complete(null);
                                    return CompletableFuture.<Void>completedFuture(null);
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
}
