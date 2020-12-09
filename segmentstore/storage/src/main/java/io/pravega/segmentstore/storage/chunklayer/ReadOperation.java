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
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_READ_BYTES;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_READ_LATENCY;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_READ_INDEX_SCAN_LATENCY;

@Slf4j
class ReadOperation implements Callable<CompletableFuture<Integer>> {
    private final SegmentHandle handle;
    private final long offset;
    private final byte[] buffer;
    private final int bufferOffset;
    private final int length;
    private final ChunkedSegmentStorage chunkedSegmentStorage;
    private final long traceId;
    private final Timer timer;
    private volatile SegmentMetadata segmentMetadata;
    private final AtomicInteger bytesRemaining = new AtomicInteger();
    private final AtomicInteger currentBufferOffset = new AtomicInteger();
    private final AtomicLong currentOffset = new AtomicLong();
    private final AtomicInteger totalBytesRead = new AtomicInteger();
    private final AtomicLong startOffsetForCurrentChunk = new AtomicLong();
    private volatile String currentChunkName;
    private volatile ChunkMetadata chunkToReadFrom = null;
    private volatile boolean isLoopExited;
    private final AtomicInteger cntScanned = new AtomicInteger();
    private volatile int bytesToRead;

    ReadOperation(ChunkedSegmentStorage chunkedSegmentStorage, SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) {
        this.handle = handle;
        this.offset = offset;
        this.buffer = buffer;
        this.bufferOffset = bufferOffset;
        this.length = length;
        this.chunkedSegmentStorage = chunkedSegmentStorage;
        traceId = LoggerHelpers.traceEnter(log, "read", handle, offset, length);
        timer = new Timer();
    }

    public CompletableFuture<Integer> call() {
        // Validate preconditions.
        checkPreconditions();
        log.debug("{} read - started op={}, segment={}, offset={}, length={}.",
                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, length);
        val streamSegmentName = handle.getSegmentName();
        return ChunkedSegmentStorage.tryWith(chunkedSegmentStorage.getMetadataStore().beginTransaction(true, streamSegmentName),
                txn -> txn.get(streamSegmentName)
                        .thenComposeAsync(storageMetadata -> {
                            segmentMetadata = (SegmentMetadata) storageMetadata;

                            // Validate preconditions.
                            checkState();

                            if (length == 0) {
                                return CompletableFuture.completedFuture(0);
                            }

                            return findChunkForOffset(txn)
                                    .thenComposeAsync(v -> {
                                        // Now read.
                                        return readData(txn);
                                    }, chunkedSegmentStorage.getExecutor())
                                    .exceptionally(ex -> {
                                        log.debug("{} read - exception op={}, segment={}, offset={}, bytesRead={}.",
                                                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, totalBytesRead);
                                        if (ex instanceof CompletionException) {
                                            throw (CompletionException) ex;
                                        }
                                        throw new CompletionException(ex);
                                    })
                                    .thenApplyAsync(v -> {
                                        logEnd();
                                        return totalBytesRead.get();
                                    }, chunkedSegmentStorage.getExecutor());
                        }, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor());
    }

    private void logEnd() {
        Duration elapsed = timer.getElapsed();
        SLTS_READ_LATENCY.reportSuccessEvent(elapsed);
        SLTS_READ_BYTES.add(length);
        if (chunkedSegmentStorage.getConfig().getLateWarningThresholdInMillis() < elapsed.toMillis()) {
            log.warn("{} read - late op={}, segment={}, offset={}, bytesRead={}, latency={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, totalBytesRead, elapsed.toMillis());
        } else {
            log.debug("{} read - finished op={}, segment={}, offset={}, bytesRead={}, latency={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, totalBytesRead, elapsed.toMillis());
        }
        LoggerHelpers.traceLeave(log, "read", traceId, handle, offset, totalBytesRead);
    }

    private CompletableFuture<Void> readData(MetadataTransaction txn) {
        return Futures.loop(
                () -> bytesRemaining.get() > 0 && null != currentChunkName,
                () -> {
                    Preconditions.checkState(null != chunkToReadFrom, "chunkToReadFrom is null");
                    bytesToRead = Math.min(bytesRemaining.get(), Math.toIntExact(chunkToReadFrom.getLength() - (currentOffset.get() - startOffsetForCurrentChunk.get())));
                    if (currentOffset.get() >= startOffsetForCurrentChunk.get() + chunkToReadFrom.getLength()) {
                        // The current chunk is over. Move to the next one.
                        currentChunkName = chunkToReadFrom.getNextChunk();
                        if (null != currentChunkName) {
                            startOffsetForCurrentChunk.addAndGet(chunkToReadFrom.getLength());
                            return txn.get(currentChunkName)
                                    .thenAcceptAsync(storageMetadata -> {
                                        chunkToReadFrom = (ChunkMetadata) storageMetadata;
                                        Preconditions.checkState(null != chunkToReadFrom, "chunkToReadFrom is null");
                                        log.debug("{} read - reading from next chunk - op={}, segment={}, chunk={}", chunkedSegmentStorage.getLogPrefix(),
                                                System.identityHashCode(this), handle.getSegmentName(), chunkToReadFrom);
                                    }, chunkedSegmentStorage.getExecutor());
                        }
                    } else {
                        Preconditions.checkState(bytesToRead != 0, "bytesToRead is 0");
                        // Read data from the chunk.
                        return chunkedSegmentStorage.getChunkStorage().openRead(chunkToReadFrom.getName())
                                .thenComposeAsync(chunkHandle ->
                                        chunkedSegmentStorage.getChunkStorage().read(chunkHandle,
                                                currentOffset.get() - startOffsetForCurrentChunk.get(),
                                                bytesToRead,
                                                buffer,
                                                currentBufferOffset.get())
                                                .thenAcceptAsync(bytesRead -> {
                                                    bytesRemaining.addAndGet(-bytesRead);
                                                    currentOffset.addAndGet(bytesRead);
                                                    currentBufferOffset.addAndGet(bytesRead);
                                                    totalBytesRead.addAndGet(bytesRead);
                                                }, chunkedSegmentStorage.getExecutor()),
                                        chunkedSegmentStorage.getExecutor());
                    }
                    return CompletableFuture.completedFuture(null);
                }, chunkedSegmentStorage.getExecutor());
    }

    private CompletableFuture<Void> findChunkForOffset(MetadataTransaction txn) {

        currentChunkName = segmentMetadata.getFirstChunk();
        chunkToReadFrom = null;

        Preconditions.checkState(null != currentChunkName, "currentChunkName must not be null.");

        bytesRemaining.set(length);
        currentBufferOffset.set(bufferOffset);
        currentOffset.set(offset);
        totalBytesRead.set(0);

        // Find the first chunk that contains the data.
        startOffsetForCurrentChunk.set(segmentMetadata.getFirstChunkStartOffset());
        val readIndexTimer = new Timer();

        if (offset > segmentMetadata.getLastChunkStartOffset()) {
            startOffsetForCurrentChunk.set(segmentMetadata.getLastChunkStartOffset());
            currentChunkName = segmentMetadata.getLastChunk();
        } else {
            // Find the name of the chunk in the cached read index that is floor to required offset.
            val floorEntry = chunkedSegmentStorage.getReadIndexCache().findFloor(handle.getSegmentName(), offset);
            if (null != floorEntry && startOffsetForCurrentChunk.get() < floorEntry.getOffset() && null != floorEntry.getChunkName()) {
                startOffsetForCurrentChunk.set(floorEntry.getOffset());
                currentChunkName = floorEntry.getChunkName();
            }
        }
        // Navigate to the chunk that contains the first byte of requested data.
        return Futures.loop(
                () -> currentChunkName != null && !isLoopExited,
                () -> txn.get(currentChunkName)
                        .thenAcceptAsync(storageMetadata -> {
                            chunkToReadFrom = (ChunkMetadata) storageMetadata;
                            Preconditions.checkState(null != chunkToReadFrom, "chunkToReadFrom is null");
                            if (startOffsetForCurrentChunk.get() <= currentOffset.get()
                                    && startOffsetForCurrentChunk.get() + chunkToReadFrom.getLength() > currentOffset.get()) {
                                // we have found a chunk that contains first byte we want to read
                                log.debug("{} read - found chunk to read - op={}, segment={}, chunk={}, startOffset={}, length={}, readOffset={}.",
                                        chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this),
                                        handle.getSegmentName(), chunkToReadFrom, startOffsetForCurrentChunk, chunkToReadFrom.getLength(), currentOffset);
                                isLoopExited = true;
                                return;
                            }
                            currentChunkName = chunkToReadFrom.getNextChunk();
                            startOffsetForCurrentChunk.addAndGet(chunkToReadFrom.getLength());

                            // Update read index with newly visited chunk.
                            if (null != currentChunkName) {
                                chunkedSegmentStorage.getReadIndexCache().addIndexEntry(handle.getSegmentName(), currentChunkName, startOffsetForCurrentChunk.get());
                            }
                            cntScanned.incrementAndGet();
                        }, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor())
                .thenAcceptAsync(v -> {
                    val elapsed = readIndexTimer.getElapsed();
                    SLTS_READ_INDEX_SCAN_LATENCY.reportSuccessEvent(elapsed);
                    log.debug("{} read - chunk lookup - op={}, segment={}, offset={}, scanned={}, latency={}.",
                            chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this),
                            handle.getSegmentName(), offset, cntScanned.get(), elapsed.toMillis());
                }, chunkedSegmentStorage.getExecutor());
    }

    private void checkState() {
        chunkedSegmentStorage.checkSegmentExists(handle.getSegmentName(), segmentMetadata);

        segmentMetadata.checkInvariants();

        Preconditions.checkArgument(offset < segmentMetadata.getLength(), "Offset %s is beyond the last offset %s of the segment %s.",
                offset, segmentMetadata.getLength(), handle.getSegmentName());

        if (offset < segmentMetadata.getStartOffset()) {
            throw new CompletionException(new StreamSegmentTruncatedException(handle.getSegmentName(), segmentMetadata.getStartOffset(), offset));
        }
    }

    private void checkPreconditions() {
        Preconditions.checkNotNull(handle, "handle");
        Preconditions.checkNotNull(buffer, "buffer");
        Preconditions.checkNotNull(handle.getSegmentName(), "streamSegmentName");

        Exceptions.checkArrayRange(bufferOffset, length, buffer.length, "bufferOffset", "length");

        if (offset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    offset, bufferOffset, length, buffer.length));
        }
    }
}
