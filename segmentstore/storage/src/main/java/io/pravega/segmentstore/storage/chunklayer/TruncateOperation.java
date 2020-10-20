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
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadataWritesFencedOutException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_TRUNCATE_COUNT;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_TRUNCATE_LATENCY;

/**
 * Implements truncate operation.
 */
@Slf4j
class TruncateOperation implements Callable<CompletableFuture<Void>> {
    private final SegmentHandle handle;
    private final long offset;
    private final ChunkedSegmentStorage chunkedSegmentStorage;

    private volatile String currentChunkName;
    private volatile ChunkMetadata currentMetadata;
    private volatile long oldLength;
    private volatile long startOffset;
    private final ArrayList<String> chunksToDelete = new ArrayList<>();
    private volatile SegmentMetadata segmentMetadata;
    private volatile String streamSegmentName;

    private volatile boolean isLoopExited;
    private volatile long traceId;
    private volatile Timer timer;

    TruncateOperation(ChunkedSegmentStorage chunkedSegmentStorage, SegmentHandle handle, long offset) {
        this.handle = handle;
        this.offset = offset;
        this.chunkedSegmentStorage = chunkedSegmentStorage;
    }

    public CompletableFuture<Void> call() {
        traceId = LoggerHelpers.traceEnter(log, "truncate", handle, offset);
        timer = new Timer();

        checkPreconditions();
        log.debug("{} truncate - started op={}, segment={}, offset={}.",
                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset);

        streamSegmentName = handle.getSegmentName();
        return ChunkedSegmentStorage.tryWith(chunkedSegmentStorage.getMetadataStore().beginTransaction(streamSegmentName), txn ->
                txn.get(streamSegmentName)
                        .thenComposeAsync(storageMetadata -> {
                            segmentMetadata = (SegmentMetadata) storageMetadata;
                            // Check preconditions
                            checkPreconditions(streamSegmentName, segmentMetadata);

                            if (segmentMetadata.getStartOffset() == offset) {
                                // Nothing to do
                                return CompletableFuture.completedFuture(null);
                            }

                            return updateFirstChunk(txn)
                                    .thenComposeAsync(v -> {
                                        deleteChunks(txn);

                                        txn.update(segmentMetadata);

                                        // Check invariants.
                                        Preconditions.checkState(segmentMetadata.getLength() == oldLength, "truncate should not change segment length");
                                        segmentMetadata.checkInvariants();

                                        // Finally commit.
                                        return commit(txn)
                                                .handleAsync(this::handleException, chunkedSegmentStorage.getExecutor())
                                                .thenComposeAsync(vv ->
                                                                chunkedSegmentStorage.collectGarbage(chunksToDelete).thenApplyAsync(vvv -> {
                                                                    postCommit();
                                                                    return null;
                                                                }, chunkedSegmentStorage.getExecutor()),
                                                        chunkedSegmentStorage.getExecutor());
                                    }, chunkedSegmentStorage.getExecutor());
                        }, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor());
    }

    private void postCommit() {
        // Update the read index by removing all entries below truncate offset.
        chunkedSegmentStorage.getReadIndexCache().truncateReadIndex(streamSegmentName, segmentMetadata.getStartOffset());

        logEnd();
    }

    private void logEnd() {
        val elapsed = timer.getElapsed();
        SLTS_TRUNCATE_LATENCY.reportSuccessEvent(elapsed);
        SLTS_TRUNCATE_COUNT.inc();
        if (chunkedSegmentStorage.getConfig().getLateWarningThresholdInMillis() < elapsed.toMillis()) {
            log.warn("{} truncate - late op={}, segment={}, offset={}, latency={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, elapsed.toMillis());
        } else {
            log.debug("{} truncate - finished op={}, segment={}, offset={}, latency={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, elapsed.toMillis());
        }
        LoggerHelpers.traceLeave(log, "truncate", traceId, handle, offset);
    }

    private Void handleException(Void value, Throwable e) {
        if (null != e) {
            log.debug("{} truncate - exception op={}, segment={}, offset={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset);
            val ex = Exceptions.unwrap(e);
            if (ex instanceof StorageMetadataWritesFencedOutException) {
                throw new CompletionException(new StorageNotPrimaryException(streamSegmentName, ex));
            }
            throw new CompletionException(ex);
        }
        return value;
    }

    private CompletableFuture<Void> commit(MetadataTransaction txn) {
        // Commit system logs.
        if (chunkedSegmentStorage.isStorageSystemSegment(segmentMetadata)) {
            val finalStartOffset = startOffset;
            txn.setExternalCommitStep(() -> {
                chunkedSegmentStorage.getSystemJournal().commitRecord(
                        SystemJournal.TruncationRecord.builder()
                                .segmentName(streamSegmentName)
                                .offset(offset)
                                .firstChunkName(segmentMetadata.getFirstChunk())
                                .startOffset(finalStartOffset)
                                .build());
                return null;
            });
        }

        // Finally commit.
        return txn.commit();
    }

    private CompletableFuture<Void> updateFirstChunk(MetadataTransaction txn) {
        currentChunkName = segmentMetadata.getFirstChunk();
        oldLength = segmentMetadata.getLength();
        startOffset = segmentMetadata.getFirstChunkStartOffset();
        return Futures.loop(
                () -> currentChunkName != null && !isLoopExited,
                () -> txn.get(currentChunkName)
                        .thenApplyAsync(storageMetadata -> {
                            currentMetadata = (ChunkMetadata) storageMetadata;
                            Preconditions.checkState(null != currentMetadata, "currentMetadata is null.");

                            // If for given chunk start <= offset < end  then we have found the chunk that will be the first chunk.
                            if ((startOffset <= offset) && (startOffset + currentMetadata.getLength() > offset)) {
                                isLoopExited = true;
                                return null;
                            }

                            startOffset += currentMetadata.getLength();
                            chunksToDelete.add(currentMetadata.getName());
                            segmentMetadata.decrementChunkCount();

                            // move to next chunk
                            currentChunkName = currentMetadata.getNextChunk();
                            return null;
                        }, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor()
        ).thenApplyAsync(v -> {
            segmentMetadata.setFirstChunk(currentChunkName);
            segmentMetadata.setStartOffset(offset);
            segmentMetadata.setFirstChunkStartOffset(startOffset);
            return null;
        }, chunkedSegmentStorage.getExecutor());
    }

    private void deleteChunks(MetadataTransaction txn) {
        for (String toDelete : chunksToDelete) {
            txn.delete(toDelete);
            // Adjust last chunk if required.
            if (toDelete.equals(segmentMetadata.getLastChunk())) {
                segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength());
                segmentMetadata.setLastChunk(null);
            }
        }
    }

    private void checkPreconditions(String streamSegmentName, SegmentMetadata segmentMetadata) {
        chunkedSegmentStorage.checkSegmentExists(streamSegmentName, segmentMetadata);
        // This check seems wrong and should be removed.
        // chunkedSegmentStorage.checkNotSealed(streamSegmentName, segmentMetadata);
        chunkedSegmentStorage.checkOwnership(streamSegmentName, segmentMetadata);

        if (segmentMetadata.getLength() < offset || segmentMetadata.getStartOffset() > offset) {
            throw new IllegalArgumentException(String.format("offset %d is outside of valid range [%d, %d) for segment %s",
                    offset, segmentMetadata.getStartOffset(), segmentMetadata.getLength(), streamSegmentName));
        }
    }

    private void checkPreconditions() {
        Preconditions.checkArgument(null != handle, "handle");
        Preconditions.checkArgument(!handle.isReadOnly(), "handle");
        Preconditions.checkArgument(offset >= 0, "offset");
    }
}
