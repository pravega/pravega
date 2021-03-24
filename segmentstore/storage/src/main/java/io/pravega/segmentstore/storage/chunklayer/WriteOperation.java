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
package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.BoundedInputStream;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadataWritesFencedOutException;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_WRITE_BYTES;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_WRITE_LATENCY;

/**
 * Implements the write operation.
 */
@Slf4j
class WriteOperation implements Callable<CompletableFuture<Void>> {
    private final SegmentHandle handle;
    private final long offset;
    private final InputStream data;
    private final int length;
    private final ChunkedSegmentStorage chunkedSegmentStorage;
    private final long traceId;
    private final Timer timer;
    private final List<SystemJournal.SystemJournalRecord> systemLogRecords = Collections.synchronizedList(new ArrayList<>());
    private final List<ChunkNameOffsetPair> newReadIndexEntries = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger chunksAddedCount = new AtomicInteger();

    private volatile boolean isCommitted = false;
    private volatile SegmentMetadata segmentMetadata;
    private volatile boolean isSystemSegment;

    // Check if this is a first write after ownership changed.
    private volatile boolean isFirstWriteAfterFailover;
    private volatile boolean skipOverFailedChunk;

    private final AtomicReference<ChunkMetadata> lastChunkMetadata = new AtomicReference<>(null);
    private volatile ChunkHandle chunkHandle = null;
    private final AtomicLong bytesRemaining = new AtomicLong();
    private final AtomicLong currentOffset = new AtomicLong();

    private volatile boolean didSegmentLayoutChange = false;

    WriteOperation(ChunkedSegmentStorage chunkedSegmentStorage, SegmentHandle handle, long offset, InputStream data, int length) {
        this.handle = handle;
        this.offset = offset;
        this.data = data;
        this.length = length;
        this.chunkedSegmentStorage = chunkedSegmentStorage;
        traceId = LoggerHelpers.traceEnter(log, "write", handle, offset, length);
        timer = new Timer();
    }

    public CompletableFuture<Void> call() {
        // Validate preconditions.
        checkPreconditions();
        log.debug("{} write - started op={}, segment={}, offset={} length={}.",
                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, length);

        val streamSegmentName = handle.getSegmentName();
        return ChunkedSegmentStorage.tryWith(chunkedSegmentStorage.getMetadataStore().beginTransaction(false, handle.getSegmentName()),
                txn -> {
                    didSegmentLayoutChange = false;

                    // Retrieve metadata.
                    return txn.get(streamSegmentName)
                            .thenComposeAsync(storageMetadata -> {
                                segmentMetadata = (SegmentMetadata) storageMetadata;
                                // Validate preconditions.
                                checkState();

                                isSystemSegment = chunkedSegmentStorage.isStorageSystemSegment(segmentMetadata);

                                // Check if this is a first write after ownership changed.
                                isFirstWriteAfterFailover = segmentMetadata.isOwnershipChanged();

                                lastChunkMetadata.set(null);
                                chunkHandle = null;
                                bytesRemaining.set(length);
                                currentOffset.set(offset);

                                // Get the last chunk segmentMetadata for the segment.

                                return getLastChunk(txn)
                                        .thenComposeAsync(v ->
                                                        writeData(txn)
                                                                .thenComposeAsync(vv ->
                                                                                commit(txn)
                                                                                        .thenApplyAsync(vvvv ->
                                                                                                postCommit(), chunkedSegmentStorage.getExecutor())
                                                                                        .exceptionally(this::handleException),
                                                                        chunkedSegmentStorage.getExecutor())
                                                                .whenCompleteAsync((value, e) -> collectGarbage(), chunkedSegmentStorage.getExecutor())
                                                                .thenRunAsync(this::logEnd, chunkedSegmentStorage.getExecutor()),
                                                chunkedSegmentStorage.getExecutor());
                            }, chunkedSegmentStorage.getExecutor());
                }, chunkedSegmentStorage.getExecutor());
    }

    private Object handleException(Throwable e) {
        log.debug("{} write - exception op={}, segment={}, offset={}, length={}.",
                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, length);
        val ex = Exceptions.unwrap(e);
        if (ex instanceof StorageMetadataWritesFencedOutException) {
            throw new CompletionException(new StorageNotPrimaryException(handle.getSegmentName(), ex));
        }
        throw new CompletionException(ex);
    }

    private Object postCommit() {
        // Post commit actions.
        // Update the read index.
        chunkedSegmentStorage.getReadIndexCache().addIndexEntries(handle.getSegmentName(), newReadIndexEntries);
        return null;
    }

    private CompletableFuture<Void> getLastChunk(MetadataTransaction txn) {
        if (null != segmentMetadata.getLastChunk()) {
            return txn.get(segmentMetadata.getLastChunk())
                    .thenAcceptAsync(storageMetadata1 -> lastChunkMetadata.set((ChunkMetadata) storageMetadata1), chunkedSegmentStorage.getExecutor());
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private void logEnd() {
        val elapsed = timer.getElapsed();
        SLTS_WRITE_LATENCY.reportSuccessEvent(elapsed);
        SLTS_WRITE_BYTES.add(length);
        if (chunkedSegmentStorage.getConfig().getLateWarningThresholdInMillis() < elapsed.toMillis()) {
            log.warn("{} write - late op={}, segment={}, offset={}, length={}, latency={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, length, elapsed.toMillis());
        } else {
            log.debug("{} write - finished op={}, segment={}, offset={}, length={}, latency={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, length, elapsed.toMillis());
        }
        LoggerHelpers.traceLeave(log, "write", traceId, handle, offset);
    }

    private void collectGarbage() {
        if (!isCommitted && chunksAddedCount.get() > 0) {
            // Collect garbage.
            chunkedSegmentStorage.getGarbageCollector().addToGarbage(newReadIndexEntries.stream().map(ChunkNameOffsetPair::getChunkName).collect(Collectors.toList()));
        }
    }

    private CompletableFuture<Void> commit(MetadataTransaction txn) {
        // commit all system log records if required.
        if (isSystemSegment && chunksAddedCount.get() > 0) {
            // commit all system log records.
            Preconditions.checkState(chunksAddedCount.get() == systemLogRecords.size(),
                    "Number of chunks added (%s) must match number of system log records(%s)", chunksAddedCount.get(), systemLogRecords.size());
            txn.setExternalCommitStep(() -> {
                chunkedSegmentStorage.getSystemJournal().commitRecords(systemLogRecords);
                return null;
            });
        }

        // if layout did not change then commit with lazyWrite.
        return txn.commit(!didSegmentLayoutChange && chunkedSegmentStorage.getConfig().isLazyCommitEnabled())
                .thenRunAsync(() -> isCommitted = true, chunkedSegmentStorage.getExecutor());

    }

    private CompletableFuture<Void> writeData(MetadataTransaction txn) {
        return Futures.loop(
                () -> bytesRemaining.get() > 0,
                () -> {
                    // Check if new chunk needs to be added.
                    // This could be either because there are no existing chunks or last chunk has reached max rolling length.
                    return openChunkToWrite(txn)
                            .thenComposeAsync(v -> {
                                // Calculate the data that needs to be written.
                                val oldOffset = currentOffset.get();
                                val offsetToWriteAt = currentOffset.get() - segmentMetadata.getLastChunkStartOffset();
                                val writeSize = (int) Math.min(bytesRemaining.get(), segmentMetadata.getMaxRollinglength() - offsetToWriteAt);

                                // Write data to last chunk.
                                return writeToChunk(txn,
                                        segmentMetadata,
                                        data,
                                        chunkHandle,
                                        lastChunkMetadata.get(),
                                        offsetToWriteAt,
                                        writeSize)
                                .thenRunAsync(() -> {
                                    // Update block index.
                                    if (!segmentMetadata.isStorageSystemSegment()) {
                                        chunkedSegmentStorage.addBlockIndexEntriesForChunk(txn,
                                                segmentMetadata.getName(),
                                                chunkHandle.getChunkName(),
                                                segmentMetadata.getLastChunkStartOffset(),
                                                oldOffset,
                                                segmentMetadata.getLength());
                                    }
                                }, chunkedSegmentStorage.getExecutor());
                            }, chunkedSegmentStorage.getExecutor());
                }, chunkedSegmentStorage.getExecutor())
                .thenRunAsync(() -> {
                    // Check invariants.
                    segmentMetadata.checkInvariants();
                }, chunkedSegmentStorage.getExecutor());
    }

    private CompletableFuture<Void> openChunkToWrite(MetadataTransaction txn) {
        if (null == lastChunkMetadata.get()
                || (lastChunkMetadata.get().getLength() >= segmentMetadata.getMaxRollinglength())
                || isFirstWriteAfterFailover
                || skipOverFailedChunk
                || !chunkedSegmentStorage.shouldAppend()) {
            return addNewChunk(txn);

        } else {
            // No new chunk needed just write data to existing chunk.
            chunkHandle = ChunkHandle.writeHandle(lastChunkMetadata.get().getName());
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> addNewChunk(MetadataTransaction txn) {
        // Create new chunk
        String newChunkName = getNewChunkName(handle.getSegmentName(),
                segmentMetadata.getLength());
        CompletableFuture<ChunkHandle> createdHandle;
        if (chunkedSegmentStorage.shouldAppend()) {
            createdHandle = chunkedSegmentStorage.getChunkStorage().create(newChunkName);
        } else {
            createdHandle = CompletableFuture.completedFuture(ChunkHandle.writeHandle(newChunkName));
        }
        return createdHandle
                .thenAcceptAsync(h -> {
                    chunkHandle = h;
                    String previousLastChunkName = lastChunkMetadata.get() == null ? null : lastChunkMetadata.get().getName();

                    // update first and last chunks.
                    lastChunkMetadata.set(updateMetadataForChunkAddition(txn,
                            segmentMetadata,
                            newChunkName,
                            isFirstWriteAfterFailover,
                            lastChunkMetadata.get()));

                    // Record the creation of new chunk.
                    if (isSystemSegment) {
                        addSystemLogRecord(systemLogRecords,
                                handle.getSegmentName(),
                                segmentMetadata.getLength(),
                                previousLastChunkName,
                                newChunkName);
                        txn.markPinned(lastChunkMetadata.get());
                    }
                    // Update read index.
                    newReadIndexEntries.add(new ChunkNameOffsetPair(segmentMetadata.getLength(), newChunkName));

                    isFirstWriteAfterFailover = false;
                    skipOverFailedChunk = false;
                    didSegmentLayoutChange = true;
                    chunksAddedCount.incrementAndGet();

                    log.debug("{} write - New chunk added - op={}, segment={}, chunk={}, offset={}.",
                            chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), newChunkName, segmentMetadata.getLength());
                }, chunkedSegmentStorage.getExecutor());
    }

    private void checkState() {
        val streamSegmentName = handle.getSegmentName();
        chunkedSegmentStorage.checkSegmentExists(streamSegmentName, segmentMetadata);
        segmentMetadata.checkInvariants();
        chunkedSegmentStorage.checkNotSealed(streamSegmentName, segmentMetadata);
        chunkedSegmentStorage.checkOwnership(streamSegmentName, segmentMetadata);

        // Validate that offset is correct.
        if ((segmentMetadata.getLength()) != offset) {
            throw new CompletionException(new BadOffsetException(handle.getSegmentName(), segmentMetadata.getLength(), offset));
        }
    }

    private void checkPreconditions() {
        Preconditions.checkArgument(null != data, "data must not be null");
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read only. Segment = %s", handle.getSegmentName());
        Preconditions.checkArgument(offset >= 0, "offset must be non negative. Segment = %s", handle.getSegmentName());
        Preconditions.checkArgument(length >= 0, "length must be non negative. Segment = %s", handle.getSegmentName());
    }

    private String getNewChunkName(String segmentName, long offset) {
        return NameUtils.getSegmentChunkName(segmentName, chunkedSegmentStorage.getEpoch(), offset);
    }

    /**
     * Updates the segment metadata for the newly added chunk.
     */
    private ChunkMetadata updateMetadataForChunkAddition(MetadataTransaction txn,
                                                         SegmentMetadata segmentMetadata,
                                                         String newChunkName,
                                                         boolean isFirstWriteAfterFailover,
                                                         ChunkMetadata lastChunkMetadata) {
        ChunkMetadata newChunkMetadata = ChunkMetadata.builder()
                .name(newChunkName)
                .build();
        newChunkMetadata.setActive(true);
        segmentMetadata.setLastChunk(newChunkName);
        if (lastChunkMetadata == null) {
            segmentMetadata.setFirstChunk(newChunkName);
        } else {
            lastChunkMetadata.setNextChunk(newChunkName);
            txn.update(lastChunkMetadata);
        }
        segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength());

        // Reset ownershipChanged flag after first write is done.
        if (isFirstWriteAfterFailover) {
            segmentMetadata.setOwnerEpoch(chunkedSegmentStorage.getEpoch());
            segmentMetadata.setOwnershipChanged(false);
            log.debug("{} write - First write after failover - op={}, segment={}.", chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), segmentMetadata.getName());
        }
        segmentMetadata.setChunkCount(segmentMetadata.getChunkCount() + 1);

        // Update the transaction.
        txn.create(newChunkMetadata);
        txn.update(segmentMetadata);
        return newChunkMetadata;
    }

    /**
     * Adds a system log.
     *
     * @param systemLogRecords  List of system records.
     * @param streamSegmentName Name of the segment.
     * @param offset            Offset at which new chunk was added.
     * @param oldChunkName      Name of the previous last chunk.
     * @param newChunkName      Name of the new last chunk.
     */
    private void addSystemLogRecord(List<SystemJournal.SystemJournalRecord> systemLogRecords, String streamSegmentName, long offset, String oldChunkName, String newChunkName) {
        systemLogRecords.add(
                SystemJournal.ChunkAddedRecord.builder()
                        .segmentName(streamSegmentName)
                        .offset(offset)
                        .oldChunkName(oldChunkName)
                        .newChunkName(newChunkName)
                        .build());
    }

    /**
     * Write to chunk.
     */
    private CompletableFuture<Void> writeToChunk(MetadataTransaction txn,
                                                    SegmentMetadata segmentMetadata,
                                                    InputStream data,
                                                    ChunkHandle chunkHandle,
                                                    ChunkMetadata chunkWrittenMetadata,
                                                    long offsetToWriteAt,
                                                    int bytesCount) {
        Preconditions.checkState(0 != bytesCount, "Attempt to write zero bytes. Segment=%s Chunk=%s offsetToWriteAt=%s", segmentMetadata, chunkWrittenMetadata, offsetToWriteAt);
        // Finally write the data.
        val bis = new BoundedInputStream(data, bytesCount);
        CompletableFuture<Integer> retValue;
        if (chunkedSegmentStorage.shouldAppend()) {
            retValue = chunkedSegmentStorage.getChunkStorage().write(chunkHandle, offsetToWriteAt, bytesCount, bis);
        } else {
            retValue = chunkedSegmentStorage.getChunkStorage().createWithContent(chunkHandle.getChunkName(), bytesCount, bis)
                    .thenApplyAsync(h -> bytesCount, chunkedSegmentStorage.getExecutor());
        }
        return retValue
                .thenAcceptAsync(bytesWritten -> {
                    // Update the metadata for segment and chunk.
                    Preconditions.checkState(bytesWritten >= 0, "bytesWritten (%s) must be non-negative. Segment=%s Chunk=%s offsetToWriteAt=%s",
                            bytesWritten, segmentMetadata, chunkWrittenMetadata, offsetToWriteAt);
                    segmentMetadata.setLength(segmentMetadata.getLength() + bytesWritten);
                    chunkWrittenMetadata.setLength(chunkWrittenMetadata.getLength() + bytesWritten);
                    txn.update(chunkWrittenMetadata);
                    txn.update(segmentMetadata);
                    bytesRemaining.addAndGet(-bytesWritten);
                    currentOffset.addAndGet(bytesWritten);
                }, chunkedSegmentStorage.getExecutor())
                .handleAsync((v, e) -> {
                    if (null != e) {
                        val ex = Exceptions.unwrap(e);
                        if (ex instanceof InvalidOffsetException) {
                            val invalidEx = (InvalidOffsetException) ex;
                            // if the length of chunk on the LTS is greater then just skip this chunk.
                            // This could happen if the previous write failed while writing data and chunk was partially written.
                            if (invalidEx.getExpectedOffset() > offsetToWriteAt) {
                                skipOverFailedChunk = true;
                                log.debug("{} write - skipping partially written chunk op={}, segment={}, chunk={} expected={} given={}.",
                                        chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(),
                                        chunkHandle.getChunkName(), invalidEx.getExpectedOffset(), invalidEx.getGivenOffset());
                                return null;
                            }
                            throw new CompletionException(new BadOffsetException(segmentMetadata.getName(),
                                    currentOffset.get() + ((InvalidOffsetException) ex).getExpectedOffset(),
                                    currentOffset.get() + ((InvalidOffsetException) ex).getGivenOffset()));
                        }

                        throw new CompletionException(ex);
                    }
                    return v;
                }, chunkedSegmentStorage.getExecutor());
    }
}
