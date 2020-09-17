/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
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

    private final ArrayList<SystemJournal.SystemJournalRecord> systemLogRecords = new ArrayList<>();
    private final List<ChunkNameOffsetPair> newReadIndexEntries = new ArrayList<>();
    private final AtomicInteger chunksAddedCount = new AtomicInteger();
    private volatile boolean isCommitted = false;

    private volatile long traceId;
    private volatile Timer timer;

    private volatile String streamSegmentName;
    private volatile SegmentMetadata segmentMetadata;

    private volatile boolean isSystemSegment;

    // Check if this is a first write after ownership changed.
    private volatile boolean isFirstWriteAfterFailover;

    private volatile ChunkMetadata lastChunkMetadata = null;
    private volatile ChunkHandle chunkHandle = null;
    private volatile int bytesRemaining;
    private volatile long currentOffset;

    private volatile boolean didSegmentLayoutChange = false;

    WriteOperation(ChunkedSegmentStorage chunkedSegmentStorage, SegmentHandle handle, long offset, InputStream data, int length) {
        this.handle = handle;
        this.offset = offset;
        this.data = data;
        this.length = length;
        this.chunkedSegmentStorage = chunkedSegmentStorage;
    }

    public CompletableFuture<Void> call() {
        traceId = LoggerHelpers.traceEnter(log, "write", handle, offset, length);
        timer = new Timer();

        // Validate preconditions.
        checkPreconditions();
        log.debug("{} write - started op={}, segment={}, offset={}.",
                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, length);

        streamSegmentName = handle.getSegmentName();
        return ChunkedSegmentStorage.tryWith(chunkedSegmentStorage.getMetadataStore().beginTransaction(handle.getSegmentName()),
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

                                lastChunkMetadata = null;
                                chunkHandle = null;
                                bytesRemaining = length;
                                currentOffset = offset;

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
                                                                .thenApplyAsync(vvv -> {
                                                                    logEnd();
                                                                    return null;
                                                                }, chunkedSegmentStorage.getExecutor()),
                                                chunkedSegmentStorage.getExecutor());
                            }, chunkedSegmentStorage.getExecutor());
                }, chunkedSegmentStorage.getExecutor());
    }

    private Object handleException(Throwable e) {
        log.debug("{} write - exception op={}, segment={}, offset={}, length={}.",
                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, length);
        val ex = Exceptions.unwrap(e);
        if (ex instanceof StorageMetadataWritesFencedOutException) {
            throw new CompletionException(new StorageNotPrimaryException(streamSegmentName, ex));
        }
        throw new CompletionException(ex);
    }

    private Object postCommit() {
        // Post commit actions.
        // Update the read index.
        chunkedSegmentStorage.getReadIndexCache().addIndexEntries(streamSegmentName, newReadIndexEntries);
        return null;
    }

    private CompletableFuture<Void> getLastChunk(MetadataTransaction txn) {
        if (null != segmentMetadata.getLastChunk()) {
            return txn.get(segmentMetadata.getLastChunk())
                    .thenApplyAsync(storageMetadata1 -> {
                        lastChunkMetadata = (ChunkMetadata) storageMetadata1;
                        return null;
                    }, chunkedSegmentStorage.getExecutor());
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
            chunkedSegmentStorage.collectGarbage(newReadIndexEntries.stream().map(ChunkNameOffsetPair::getChunkName).collect(Collectors.toList()));
        }
    }

    private CompletableFuture<Void> commit(MetadataTransaction txn) {
        // commit all system log records if required.
        if (isSystemSegment && chunksAddedCount.get() > 0) {
            // commit all system log records.
            Preconditions.checkState(chunksAddedCount.get() == systemLogRecords.size());
            txn.setExternalCommitStep(() -> {
                chunkedSegmentStorage.getSystemJournal().commitRecords(systemLogRecords);
                return null;
            });
        }

        // if layout did not change then commit with lazyWrite.
        return txn.commit(!didSegmentLayoutChange && chunkedSegmentStorage.getConfig().isLazyCommitEnabled())
                .thenApplyAsync(v -> {
                    isCommitted = true;
                    return null;
                }, chunkedSegmentStorage.getExecutor());

    }

    private CompletableFuture<Void> writeData(MetadataTransaction txn) {
        return Futures.loop(
                () -> bytesRemaining > 0,
                () -> {
                    // Check if new chunk needs to be added.
                    // This could be either because there are no existing chunks or last chunk has reached max rolling length.
                    return openChunkToWrite(txn)
                            .thenComposeAsync(v -> {
                                // Calculate the data that needs to be written.
                                val offsetToWriteAt = currentOffset - segmentMetadata.getLastChunkStartOffset();
                                val writeSize = (int) Math.min(bytesRemaining, segmentMetadata.getMaxRollinglength() - offsetToWriteAt);

                                // Write data to last chunk.
                                return writeToChunk(txn,
                                        segmentMetadata,
                                        data,
                                        chunkHandle,
                                        lastChunkMetadata,
                                        offsetToWriteAt,
                                        writeSize)
                                        .thenApplyAsync(bytesWritten -> {
                                            // Update the counts
                                            bytesRemaining -= bytesWritten;
                                            currentOffset += bytesWritten;
                                            return null;
                                        }, chunkedSegmentStorage.getExecutor());
                            }, chunkedSegmentStorage.getExecutor());
                }, chunkedSegmentStorage.getExecutor())
                .thenApplyAsync(v -> {
                    // Check invariants.
                    segmentMetadata.checkInvariants();
                    return null;
                }, chunkedSegmentStorage.getExecutor());
    }

    private CompletableFuture<Void> openChunkToWrite(MetadataTransaction txn) {
        if (null == lastChunkMetadata
                || (lastChunkMetadata.getLength() >= segmentMetadata.getMaxRollinglength())
                || isFirstWriteAfterFailover
                || !chunkedSegmentStorage.shouldAppend()) {
            return addNewChunk(txn);

        } else {
            // No new chunk needed just write data to existing chunk.
            return chunkedSegmentStorage.getChunkStorage().openWrite(lastChunkMetadata.getName())
                    .thenApplyAsync(h -> {
                        chunkHandle = h;
                        return null;
                    }, chunkedSegmentStorage.getExecutor());
        }
    }

    private CompletableFuture<Void> addNewChunk(MetadataTransaction txn) {
        // Create new chunk
        String newChunkName = getNewChunkName(streamSegmentName,
                segmentMetadata.getLength());
        return chunkedSegmentStorage.getChunkStorage().create(newChunkName)
                .thenApplyAsync(h -> {
                    chunkHandle = h;
                    String previousLastChunkName = lastChunkMetadata == null ? null : lastChunkMetadata.getName();

                    // update first and last chunks.
                    lastChunkMetadata = updateMetadataForChunkAddition(txn,
                            segmentMetadata,
                            newChunkName,
                            isFirstWriteAfterFailover,
                            lastChunkMetadata);

                    // Record the creation of new chunk.
                    if (isSystemSegment) {
                        addSystemLogRecord(systemLogRecords,
                                streamSegmentName,
                                segmentMetadata.getLength(),
                                previousLastChunkName,
                                newChunkName);
                        txn.markPinned(lastChunkMetadata);
                    }
                    // Update read index.
                    newReadIndexEntries.add(new ChunkNameOffsetPair(segmentMetadata.getLength(), newChunkName));

                    isFirstWriteAfterFailover = false;
                    didSegmentLayoutChange = true;
                    chunksAddedCount.incrementAndGet();

                    log.debug("{} write - New chunk added - segment={}, chunk={}, offset={}.",
                            chunkedSegmentStorage.getLogPrefix(), streamSegmentName, newChunkName, segmentMetadata.getLength());
                    return null;
                }, chunkedSegmentStorage.getExecutor());
    }

    private void checkState() {
        chunkedSegmentStorage.checkSegmentExists(streamSegmentName, segmentMetadata);
        segmentMetadata.checkInvariants();
        chunkedSegmentStorage.checkNotSealed(streamSegmentName, segmentMetadata);
        chunkedSegmentStorage.checkOwnership(streamSegmentName, segmentMetadata);

        // Validate that offset is correct.
        if ((segmentMetadata.getLength()) != offset) {
            throw new CompletionException(new BadOffsetException(streamSegmentName, segmentMetadata.getLength(), offset));
        }
    }

    private void checkPreconditions() {
        Preconditions.checkArgument(null != handle, "handle");
        Preconditions.checkArgument(null != data, "data");
        Preconditions.checkArgument(null != handle.getSegmentName(), "streamSegmentName");
        Preconditions.checkArgument(!handle.isReadOnly(), "handle");
        Preconditions.checkArgument(offset >= 0, "offset");
        Preconditions.checkArgument(length >= 0, "length");
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
            log.debug("{} write - First write after failover - segment={}.", chunkedSegmentStorage.getLogPrefix(), segmentMetadata.getName());
        }
        segmentMetadata.incrementChunkCount();

        // Update the transaction.
        txn.update(newChunkMetadata);
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
    private void addSystemLogRecord(ArrayList<SystemJournal.SystemJournalRecord> systemLogRecords, String streamSegmentName, long offset, String oldChunkName, String newChunkName) {
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
    private CompletableFuture<Integer> writeToChunk(MetadataTransaction txn,
                                                    SegmentMetadata segmentMetadata,
                                                    InputStream data,
                                                    ChunkHandle chunkHandle,
                                                    ChunkMetadata chunkWrittenMetadata,
                                                    long offsetToWriteAt,
                                                    int bytesCount) {
        Preconditions.checkState(0 != bytesCount, "Attempt to write zero bytes");
        // Finally write the data.
        val bis = new BoundedInputStream(data, bytesCount);
        return chunkedSegmentStorage.getChunkStorage().write(chunkHandle, offsetToWriteAt, bytesCount, bis)
                .thenApplyAsync(bytesWritten -> {
                    // Update the metadata for segment and chunk.
                    Preconditions.checkState(bytesWritten >= 0);
                    segmentMetadata.setLength(segmentMetadata.getLength() + bytesWritten);
                    chunkWrittenMetadata.setLength(chunkWrittenMetadata.getLength() + bytesWritten);
                    txn.update(chunkWrittenMetadata);
                    txn.update(segmentMetadata);
                    return bytesWritten;
                }, chunkedSegmentStorage.getExecutor())
                .exceptionally(e -> {
                    val ex = Exceptions.unwrap(e);
                    if (ex instanceof InvalidOffsetException) {
                        throw new CompletionException(new BadOffsetException(segmentMetadata.getName(),
                                ((InvalidOffsetException) ex).getExpectedOffset(),
                                ((InvalidOffsetException) ex).getGivenOffset()));
                    }
                    if (ex instanceof ChunkStorageException) {
                        throw new CompletionException(ex);
                    }

                    throw new CompletionException(ex);
                });
    }
}
