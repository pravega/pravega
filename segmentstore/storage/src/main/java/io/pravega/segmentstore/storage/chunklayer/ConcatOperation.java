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
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.StorageFullException;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadataWritesFencedOutException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_CONCAT_COUNT;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_CONCAT_LATENCY;

/**
 * Implements the concat operation.
 */
@Slf4j
class ConcatOperation implements Callable<CompletableFuture<Void>> {
    private final long traceId;
    private final SegmentHandle targetHandle;
    private final long offset;
    private final String sourceSegment;
    private final ChunkedSegmentStorage chunkedSegmentStorage;
    private final List<String> chunksToDelete = new Vector<>();
    private final List<ChunkNameOffsetPair> newReadIndexEntries = new Vector<>();
    private final Timer timer;

    private volatile SegmentMetadata targetSegmentMetadata;
    private volatile SegmentMetadata sourceSegmentMetadata;
    private volatile ChunkMetadata targetLastChunk;
    private volatile ChunkMetadata sourceFirstChunk;

    ConcatOperation(ChunkedSegmentStorage chunkedSegmentStorage, SegmentHandle targetHandle, long offset, String sourceSegment) {
        this.targetHandle = targetHandle;
        this.offset = offset;
        this.sourceSegment = sourceSegment;
        this.chunkedSegmentStorage = chunkedSegmentStorage;
        timer = new Timer();
        traceId = LoggerHelpers.traceEnter(log, "concat", targetHandle, offset, sourceSegment);
    }

    @Override
    public CompletableFuture<Void> call() {
        checkPreconditions();
        log.debug("{} concat - started op={}, target={}, source={}, offset={}.",
                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), targetHandle.getSegmentName(), sourceSegment, offset);

        return ChunkedSegmentStorage.tryWith(chunkedSegmentStorage.getMetadataStore().beginTransaction(false, targetHandle.getSegmentName(), sourceSegment),
                txn -> txn.get(targetHandle.getSegmentName())
                        .thenComposeAsync(storageMetadata1 -> {
                            targetSegmentMetadata = (SegmentMetadata) storageMetadata1;
                            return txn.get(sourceSegment)
                                    .thenComposeAsync(storageMetadata2 -> {
                                        sourceSegmentMetadata = (SegmentMetadata) storageMetadata2;
                                        return performConcat(txn);
                                    }, chunkedSegmentStorage.getExecutor());
                        }, chunkedSegmentStorage.getExecutor()), chunkedSegmentStorage.getExecutor())
                .exceptionally(ex -> handleException(ex));
    }

    private CompletableFuture<Void> performConcat(MetadataTransaction txn) {
        // Validate preconditions.
        checkState();
        val currentIndexOffset = targetSegmentMetadata.getLastChunkStartOffset();
        // Update list of chunks by appending sources list of chunks.
        return updateMetadata(txn).thenComposeAsync(v -> {
            // Finally defrag immediately.
            final CompletableFuture<Void> f;
            if (shouldDefrag() && null != targetLastChunk) {
                f = chunkedSegmentStorage.defrag(txn,
                        targetSegmentMetadata,
                        targetLastChunk.getName(),
                        null,
                        chunksToDelete,
                        newReadIndexEntries,
                        currentIndexOffset);
            } else {
                f = CompletableFuture.completedFuture(null);
            }
            return f.thenComposeAsync(v2 -> {
                targetSegmentMetadata.checkInvariants();
                // Collect garbage.
                return chunkedSegmentStorage.getGarbageCollector().addChunksToGarbage(txn.getVersion(), chunksToDelete)
                        .thenComposeAsync(v4 -> {
                            // Finally commit transaction.
                            return txn.commit()
                                    .exceptionally(this::handleException)
                                    .thenRunAsync(this::postCommit, chunkedSegmentStorage.getExecutor());
                        }, chunkedSegmentStorage.getExecutor());
            }, chunkedSegmentStorage.getExecutor());
        }, chunkedSegmentStorage.getExecutor());
    }

    private Void handleException(Throwable e) {
        log.debug("{} concat - exception op={}, target={}, source={}, offset={}.",
                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), targetHandle.getSegmentName(), sourceSegment, offset);
        val ex = Exceptions.unwrap(e);
        if (ex instanceof StorageMetadataWritesFencedOutException) {
            throw new CompletionException(new StorageNotPrimaryException(targetHandle.getSegmentName(), ex));
        }
        if (ex instanceof ChunkStorageFullException) {
            throw new CompletionException(new StorageFullException(targetHandle.getSegmentName(), ex));
        }
        throw new CompletionException(ex);
    }

    private void postCommit() {
            // Update the read index.
            chunkedSegmentStorage.getReadIndexCache().remove(sourceSegment);
            chunkedSegmentStorage.getReadIndexCache().addIndexEntries(targetHandle.getSegmentName(), newReadIndexEntries);
            logEnd();

    }

    private void logEnd() {
        val elapsed = timer.getElapsed();
        SLTS_CONCAT_LATENCY.reportSuccessEvent(elapsed);
        SLTS_CONCAT_COUNT.inc();
        if (chunkedSegmentStorage.getConfig().getLateWarningThresholdInMillis() < elapsed.toMillis()) {
            log.warn("{} concat - late op={}, target={}, source={}, offset={}, latency={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), targetHandle.getSegmentName(), sourceSegment, offset, elapsed.toMillis());
        } else {
            log.debug("{} concat - finished op={}, target={}, source={}, offset={}, latency={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), targetHandle.getSegmentName(), sourceSegment, offset, elapsed.toMillis());
        }
        LoggerHelpers.traceLeave(log, "concat", traceId, targetHandle, offset, sourceSegment);
    }

    private CompletableFuture<Void> updateMetadata(MetadataTransaction txn) {
        return txn.get(targetSegmentMetadata.getLastChunk())
                .thenComposeAsync(storageMetadata1 -> {
                    targetLastChunk = (ChunkMetadata) storageMetadata1;
                    return txn.get(sourceSegmentMetadata.getFirstChunk())
                            .thenAcceptAsync(storageMetadata2 -> {
                                sourceFirstChunk = (ChunkMetadata) storageMetadata2;

                                if (targetLastChunk != null) {
                                    targetLastChunk.setNextChunk(sourceFirstChunk.getName());
                                    txn.update(targetLastChunk);
                                } else {
                                    if (sourceFirstChunk != null) {
                                        targetSegmentMetadata.setFirstChunk(sourceFirstChunk.getName());
                                        txn.update(sourceFirstChunk);
                                    }
                                }

                                // Update segments' last chunk to point to the sources last segment.
                                targetSegmentMetadata.setLastChunk(sourceSegmentMetadata.getLastChunk());

                                // Update the length of segment.
                                targetSegmentMetadata.setLastChunkStartOffset(targetSegmentMetadata.getLength() + sourceSegmentMetadata.getLastChunkStartOffset());
                                targetSegmentMetadata.setLength(targetSegmentMetadata.getLength() + sourceSegmentMetadata.getLength() - sourceSegmentMetadata.getStartOffset());

                                targetSegmentMetadata.setChunkCount(targetSegmentMetadata.getChunkCount() + sourceSegmentMetadata.getChunkCount());

                                // Delete read index block entries for source.
                                // To avoid possibility of unintentional deadlock, skip this step for storage system segments.
                                if (!sourceSegmentMetadata.isStorageSystemSegment()) {
                                    chunkedSegmentStorage.deleteBlockIndexEntriesForChunk(txn, sourceSegment, sourceSegmentMetadata.getStartOffset(), sourceSegmentMetadata.getLength());
                                }
                                txn.update(targetSegmentMetadata);
                                txn.delete(sourceSegment);

                            }, chunkedSegmentStorage.getExecutor());
                }, chunkedSegmentStorage.getExecutor());
    }

    private void checkState() {
        chunkedSegmentStorage.checkSegmentExists(targetHandle.getSegmentName(), targetSegmentMetadata);
        targetSegmentMetadata.checkInvariants();
        chunkedSegmentStorage.checkNotSealed(targetHandle.getSegmentName(), targetSegmentMetadata);

        chunkedSegmentStorage.checkSegmentExists(sourceSegment, sourceSegmentMetadata);
        sourceSegmentMetadata.checkInvariants();

        // This is a critical assumption at this point which should not be broken,
        Preconditions.checkState(!targetSegmentMetadata.isStorageSystemSegment(), "Storage system segments cannot be concatenated. Segment=%s", targetSegmentMetadata.getName());
        Preconditions.checkState(!sourceSegmentMetadata.isStorageSystemSegment(), "Storage system segments cannot be concatenated. Segment=%s", sourceSegmentMetadata.getName());

        checkSealed(sourceSegmentMetadata);
        chunkedSegmentStorage.checkOwnership(targetSegmentMetadata.getName(), targetSegmentMetadata);

        if (sourceSegmentMetadata.getStartOffset() != 0) {
            throw new CompletionException(new StreamSegmentTruncatedException(sourceSegment, sourceSegmentMetadata.getLength(), 0));
        }

        if (offset != targetSegmentMetadata.getLength()) {
            throw new CompletionException(new BadOffsetException(targetHandle.getSegmentName(), targetSegmentMetadata.getLength(), offset));
        }
    }

    private void checkPreconditions() {
        Preconditions.checkArgument(!targetHandle.isReadOnly(), "targetHandle must not be read only. Segment=%s", targetHandle.getSegmentName());
        Preconditions.checkArgument(offset >= 0, "offset must be non negative. Segment=%s offset=%s", targetHandle.getSegmentName(), offset);
    }

    private void checkSealed(SegmentMetadata sourceSegmentMetadata) {
        if (!sourceSegmentMetadata.isSealed()) {
            throw new IllegalStateException("Source segment must be sealed.");
        }
    }

    private boolean shouldDefrag() {
        return (chunkedSegmentStorage.shouldAppend() || chunkedSegmentStorage.getChunkStorage().supportsConcat())
                && chunkedSegmentStorage.getConfig().isInlineDefragEnabled();
    }
}
