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
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadataWritesFencedOutException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;

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
    private final List<String> chunksToDelete = Collections.synchronizedList(new ArrayList<>());
    private final long traceId;
    private final Timer timer;
    private volatile String currentChunkName;
    private volatile ChunkMetadata currentMetadata;
    private volatile long oldLength;
    private final AtomicLong startOffset = new AtomicLong();
    private volatile SegmentMetadata segmentMetadata;
    private volatile boolean isLoopExited;

    TruncateOperation(ChunkedSegmentStorage chunkedSegmentStorage, SegmentHandle handle, long offset) {
        this.handle = handle;
        this.offset = offset;
        this.chunkedSegmentStorage = chunkedSegmentStorage;
        traceId = LoggerHelpers.traceEnter(log, "truncate", handle, offset);
        timer = new Timer();
    }

    public CompletableFuture<Void> call() {
        checkPreconditions();
        log.debug("{} truncate - started op={}, segment={}, offset={}.",
                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset);

        val streamSegmentName = handle.getSegmentName();
        return ChunkedSegmentStorage.tryWith(chunkedSegmentStorage.getMetadataStore().beginTransaction(false, streamSegmentName), txn ->
                    txn.get(streamSegmentName)
                        .thenComposeAsync(storageMetadata -> {
                            segmentMetadata = (SegmentMetadata) storageMetadata;
                            // Check preconditions
                            checkPreconditions(streamSegmentName, segmentMetadata);

                            if (segmentMetadata.getStartOffset() >= offset) {
                                // Nothing to do
                                return CompletableFuture.completedFuture(null);
                            }
                            val oldStartOffset = segmentMetadata.getStartOffset();
                            return updateFirstChunk(txn)
                                    .thenComposeAsync(v -> deleteChunks(txn)
                                            .thenComposeAsync( vvv -> {

                                                txn.update(segmentMetadata);

                                                // Check invariants.
                                                Preconditions.checkState(segmentMetadata.getLength() == oldLength,
                                                        "truncate should not change segment length. oldLength=%s Segment=%s", oldLength, segmentMetadata);
                                                segmentMetadata.checkInvariants();

                                                // Remove read index block entries.
                                                chunkedSegmentStorage.deleteBlockIndexEntriesForChunk(txn, streamSegmentName, oldStartOffset, segmentMetadata.getStartOffset());

                                                // Finally commit.
                                                return commit(txn)
                                                        .handleAsync(this::handleException, chunkedSegmentStorage.getExecutor())
                                                        .thenRunAsync(this::postCommit, chunkedSegmentStorage.getExecutor());
                                            }, chunkedSegmentStorage.getExecutor()),
                                    chunkedSegmentStorage.getExecutor());
                        }, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor());
    }

    private void postCommit() {
        // Collect garbage.
        chunkedSegmentStorage.getGarbageCollector().addToGarbage(chunksToDelete);
        // Update the read index by removing all entries below truncate offset.
        chunkedSegmentStorage.getReadIndexCache().truncateReadIndex(handle.getSegmentName(), segmentMetadata.getStartOffset());

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
                throw new CompletionException(new StorageNotPrimaryException(handle.getSegmentName(), ex));
            }
            throw new CompletionException(ex);
        }
        return value;
    }

    private CompletableFuture<Void> commit(MetadataTransaction txn) {
        // Commit system logs.
        if (chunkedSegmentStorage.isStorageSystemSegment(segmentMetadata)) {
            txn.setExternalCommitStep(() -> {
                chunkedSegmentStorage.getSystemJournal().commitRecord(
                        SystemJournal.TruncationRecord.builder()
                                .segmentName(handle.getSegmentName())
                                .offset(offset)
                                .firstChunkName(segmentMetadata.getFirstChunk())
                                .startOffset(startOffset.get())
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
        startOffset.set(segmentMetadata.getFirstChunkStartOffset());
        return Futures.loop(
                () -> currentChunkName != null && !isLoopExited,
                () -> txn.get(currentChunkName)
                        .thenAcceptAsync(storageMetadata -> {
                            currentMetadata = (ChunkMetadata) storageMetadata;
                            Preconditions.checkState(null != currentMetadata, "currentMetadata is null. Segment=%s currentChunkName=%s", segmentMetadata, currentChunkName);

                            // If for given chunk start <= offset < end  then we have found the chunk that will be the first chunk.
                            if ((startOffset.get() <= offset) && (startOffset.get() + currentMetadata.getLength() > offset)) {
                                isLoopExited = true;
                                return;
                            }

                            startOffset.addAndGet(currentMetadata.getLength());
                            chunksToDelete.add(currentMetadata.getName());
                            segmentMetadata.setChunkCount(segmentMetadata.getChunkCount() - 1);

                            // move to next chunk
                            currentChunkName = currentMetadata.getNextChunk();
                        }, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor()
        ).thenAcceptAsync(v -> {
            segmentMetadata.setFirstChunk(currentChunkName);
            segmentMetadata.setStartOffset(offset);
            segmentMetadata.setFirstChunkStartOffset(startOffset.get());
        }, chunkedSegmentStorage.getExecutor());
    }

    private CompletableFuture<Void> deleteChunks(MetadataTransaction txn) {
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String toDelete : chunksToDelete) {
            futures.add(txn.get(toDelete)
                    .thenAcceptAsync(metadata -> {
                        ((ChunkMetadata) metadata).setActive(false);
                        txn.update(metadata);
                    }, chunkedSegmentStorage.getExecutor()));
            // Adjust last chunk if required.
            if (toDelete.equals(segmentMetadata.getLastChunk())) {
                segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength());
                segmentMetadata.setLastChunk(null);
            }
        }
        return Futures.allOf(futures);
    }

    private void checkPreconditions(String streamSegmentName, SegmentMetadata segmentMetadata) {
        chunkedSegmentStorage.checkSegmentExists(streamSegmentName, segmentMetadata);
        // This check seems wrong and should be removed.
        // chunkedSegmentStorage.checkNotSealed(streamSegmentName, segmentMetadata);
        chunkedSegmentStorage.checkOwnership(streamSegmentName, segmentMetadata);

        if (segmentMetadata.getLength() < offset) {
            throw new IllegalArgumentException(String.format("offset %d is outside of valid range [%d, %d) for segment %s",
                    offset, segmentMetadata.getStartOffset(), segmentMetadata.getLength(), streamSegmentName));
        }
    }

    private void checkPreconditions() {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read only. Segment = %s", handle.getSegmentName());
        Preconditions.checkArgument(offset >= 0, "offset must be non-negative. Segment = %s offset = %s", handle.getSegmentName(), offset);
    }
}
