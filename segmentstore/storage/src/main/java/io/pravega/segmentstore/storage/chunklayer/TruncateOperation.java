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

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;

import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.*;

/**
 * Implements truncate operation.
 */
@Slf4j
class TruncateOperation implements Callable<CompletableFuture<Void>> {
    private final SegmentHandle handle;
    private final long offset;
    private final ChunkedSegmentStorage chunkedSegmentStorage;
    private final List<String> chunksToDelete = new Vector<>();
    private final long traceId;
    private final Timer timer;
    private volatile String currentChunkName;
    private volatile ChunkMetadata currentMetadata;
    private volatile long oldLength;
    private final AtomicLong startOffset = new AtomicLong();
    private volatile SegmentMetadata segmentMetadata;
    private volatile boolean isLoopExited;
    private volatile boolean isFirstChunkRelocated;

    TruncateOperation(ChunkedSegmentStorage chunkedSegmentStorage, SegmentHandle handle, long offset) {
        this.handle = handle;
        this.offset = offset;
        this.chunkedSegmentStorage = chunkedSegmentStorage;
        traceId = LoggerHelpers.traceEnter(log, "truncate", handle, offset);
        timer = new Timer();
    }

    @Override
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
                                logEnd();
                                return CompletableFuture.completedFuture(null);
                            }
                            val oldChunkCount = segmentMetadata.getChunkCount();
                            val oldStartOffset = segmentMetadata.getStartOffset();
                            return updateFirstChunk(txn)
                                    .thenComposeAsync(v -> relocateFirstChunkIfRequired(txn), chunkedSegmentStorage.getExecutor())
                                    .thenComposeAsync(v -> deleteChunks(txn)
                                            .thenComposeAsync( vvv -> {
                                                txn.update(segmentMetadata);

                                                checkInvariants(oldChunkCount);

                                                // Remove read index block entries.
                                                // To avoid possibility of unintentional deadlock, skip this step for storage system segments.
                                                if (!segmentMetadata.isStorageSystemSegment()) {
                                                    chunkedSegmentStorage.deleteBlockIndexEntriesForChunk(txn, streamSegmentName, oldStartOffset, segmentMetadata.getStartOffset());
                                                }

                                                // Collect garbage.
                                                return chunkedSegmentStorage.getGarbageCollector().addChunksToGarbage(txn.getVersion(), chunksToDelete)
                                                        .thenComposeAsync( vv ->  {
                                                            // Finally, commit.
                                                            return commit(txn)
                                                                    .handleAsync(this::handleException, chunkedSegmentStorage.getExecutor())
                                                                    .thenRunAsync(this::postCommit, chunkedSegmentStorage.getExecutor());
                                                        }, chunkedSegmentStorage.getExecutor());
                                            }, chunkedSegmentStorage.getExecutor()),
                                    chunkedSegmentStorage.getExecutor());
                        }, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor());
    }

    private void checkInvariants(final int oldChunkCount) {
        segmentMetadata.checkInvariants();
        Preconditions.checkState(segmentMetadata.getLength() == oldLength,
                "truncate should not change segment length. oldLength=%s Segment=%s", oldLength, segmentMetadata);
        Preconditions.checkState(oldChunkCount - chunksToDelete.size()  + (isFirstChunkRelocated ? 1 : 0) == segmentMetadata.getChunkCount(),
                "Number of chunks do not match. old value (%s) - number of chunks deleted (%s) + number of chunks added (%s) must match current chunk count(%s)",
                oldChunkCount, chunksToDelete.size(), segmentMetadata.getChunkCount());
        if ( isFirstChunkRelocated) {
            Preconditions.checkState(segmentMetadata.getFirstChunkStartOffset() == segmentMetadata.getStartOffset(),
                    "After relocation of first chunk FirstChunkStartOffset (%) must match StartOffset (%s)",
                    segmentMetadata.getFirstChunkStartOffset(), segmentMetadata.getStartOffset());
        }
        if (null != currentMetadata && null != segmentMetadata.getFirstChunk()) {
            Preconditions.checkState(segmentMetadata.getFirstChunk().equals(currentMetadata.getName()),
                    "First chunk name must match current metadata. Expected = %s Actual = %s", segmentMetadata.getFirstChunk(), currentMetadata.getName());
            Preconditions.checkState(segmentMetadata.getStartOffset() <= segmentMetadata.getFirstChunkStartOffset() + currentMetadata.getLength(),
                    "segment start offset (%s) must be less than or equal to first chunk start offset (%s)+ first chunk length (%s)",
                    segmentMetadata.getStartOffset(), segmentMetadata.getFirstChunkStartOffset(), currentMetadata.getLength());
            if (segmentMetadata.getChunkCount() == 1) {
                Preconditions.checkState(segmentMetadata.getLength() - segmentMetadata.getFirstChunkStartOffset() == currentMetadata.getLength(),
                        "Length of first chunk (%s) must match segment length (%s) - first chunk start offset (%s) when there is only one chunk",
                        currentMetadata.getLength(), segmentMetadata.getLength(), segmentMetadata.getFirstChunkStartOffset());
            }
        }
    }

    private CompletableFuture<Void> relocateFirstChunkIfRequired(MetadataTransaction txn) {
        if (shouldRelocate()) {
            val timer = new Timer();
            String oldChunkName = segmentMetadata.getFirstChunk();
            String newChunkName = chunkedSegmentStorage.getNewChunkName(handle.getSegmentName(), segmentMetadata.getStartOffset());
            val startOffsetInChunk = segmentMetadata.getStartOffset() - segmentMetadata.getFirstChunkStartOffset();
            val newLength = currentMetadata.getLength() - startOffsetInChunk;
            log.debug("{} truncate - relocating first chunk op={}, segment={}, offset={} old={} new={} relocatedBytes={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(),
                    offset, oldChunkName, newChunkName, newLength);
            return chunkedSegmentStorage.getGarbageCollector().trackNewChunk(txn.getVersion(), newChunkName)
                    .thenComposeAsync( v -> chunkedSegmentStorage.getChunkStorage().create(newChunkName), chunkedSegmentStorage.getExecutor())
                    .thenComposeAsync( chunkHandle -> copyBytes(chunkHandle, ChunkHandle.readHandle(oldChunkName), startOffsetInChunk, newLength), chunkedSegmentStorage.getExecutor())
                    .thenRunAsync(() -> {
                        // Create metadata for new chunk.
                        val newFirstChunkMetadata = ChunkMetadata.builder()
                                .name(newChunkName)
                                .nextChunk(currentMetadata.getNextChunk())
                                .length(newLength)
                                .build()
                                .setActive(true);
                        txn.create(newFirstChunkMetadata);

                        // Update segment metadata.
                        segmentMetadata.setFirstChunkStartOffset(segmentMetadata.getStartOffset());
                        segmentMetadata.setFirstChunk(newChunkName);

                        // Handle case when there is only one chunk
                        if (segmentMetadata.getChunkCount() == 1) {
                            segmentMetadata.setLastChunk(newChunkName);
                            segmentMetadata.setLastChunkStartOffset(segmentMetadata.getStartOffset());
                        }

                        // Mark old first chunk for deletion.
                        chunksToDelete.add(oldChunkName);
                        currentMetadata.setActive(false);

                        // Add block index entries.
                        chunkedSegmentStorage.addBlockIndexEntriesForChunk(txn,
                                segmentMetadata.getName(),
                                newChunkName,
                                segmentMetadata.getFirstChunkStartOffset(),
                                segmentMetadata.getFirstChunkStartOffset(),
                                segmentMetadata.getFirstChunkStartOffset() + newFirstChunkMetadata.getLength()
                                );

                        isFirstChunkRelocated = true;
                        currentMetadata = newFirstChunkMetadata;
                        currentChunkName = newChunkName;

                        log.debug("{} truncate - relocated first chunk op={}, segment={}, offset={} old={} new={} relocatedBytes={} time={}.",
                                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(),
                                offset, oldChunkName, newChunkName, newLength, timer.getElapsedMillis());

                    }, chunkedSegmentStorage.getExecutor());
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> copyBytes(ChunkHandle writeHandle, ChunkHandle readHandle, long startOffset, long length) {
        Preconditions.checkArgument(length <= chunkedSegmentStorage.getConfig().getMaxSizeForTruncateRelocationInbytes(),
                "size of data exceeds max size allowed for relocation. length={}, max={} ",
                length, chunkedSegmentStorage.getConfig().getMaxSizeForTruncateRelocationInbytes());
        val bytesToRead = new AtomicLong(length);
        val readAtOffset = new AtomicLong(startOffset);
        val writeAtOffset = new AtomicLong(0);
        return Futures.loop(
                () -> bytesToRead.get() > 0,
                () -> {
                    val buffer = new byte[Math.toIntExact(Math.min(chunkedSegmentStorage.getConfig().getMaxBufferSizeForChunkDataTransfer(), bytesToRead.get()))];
                    return chunkedSegmentStorage.getChunkStorage().read(readHandle, readAtOffset.get(), buffer.length, buffer, 0)
                            .thenComposeAsync(size -> {
                                bytesToRead.addAndGet(-size);
                                readAtOffset.addAndGet(size);
                                return chunkedSegmentStorage.getChunkStorage().write(writeHandle, writeAtOffset.get(), size, new ByteArrayInputStream(buffer, 0, size))
                                        .thenAcceptAsync(writeAtOffset::addAndGet, chunkedSegmentStorage.getExecutor());
                            }, chunkedSegmentStorage.getExecutor());
                },
                chunkedSegmentStorage.getExecutor()
        );
    }

    private boolean shouldRelocate() {
        return chunkedSegmentStorage.getConfig().isRelocateOnTruncateEnabled()
            && chunkedSegmentStorage.shouldAppend()
            && !chunkedSegmentStorage.isSegmentInSystemScope(handle)
            && currentMetadata.getLength() >  chunkedSegmentStorage.getConfig().getMinSizeForTruncateRelocationInbytes()
            && currentMetadata.getLength() <=  chunkedSegmentStorage.getConfig().getMaxSizeForTruncateRelocationInbytes()
            && getWastedSpacePercentage() >= chunkedSegmentStorage.getConfig().getMinPercentForTruncateRelocation();
    }

    private long getWastedSpacePercentage() {
        val wastedSpace = segmentMetadata.getStartOffset() - segmentMetadata.getFirstChunkStartOffset();
        val length = currentMetadata.getLength();
        return 0 == length ? 0 : 100 * wastedSpace / length;
    }

    private void postCommit() {
        // Update the read index by removing all entries below truncate offset.
        chunkedSegmentStorage.getReadIndexCache().truncateReadIndex(handle.getSegmentName(), segmentMetadata.getStartOffset());
        if (isFirstChunkRelocated ) {
            chunkedSegmentStorage.getReadIndexCache().addIndexEntry(handle.getSegmentName(), segmentMetadata.getFirstChunk(), segmentMetadata.getFirstChunkStartOffset());
        }
        logEnd();
    }

    private void logEnd() {
        val elapsed = timer.getElapsed();
        SLTS_TRUNCATE_LATENCY.reportSuccessEvent(elapsed);
        SLTS_TRUNCATE_COUNT.inc();
        if (segmentMetadata.isStorageSystemSegment()) {
            SLTS_SYSTEM_TRUNCATE_COUNT.inc();
            chunkedSegmentStorage.reportMetricsForSystemSegment(segmentMetadata);
        }
        if (isFirstChunkRelocated) {
            SLTS_TRUNCATE_RELOCATION_COUNT.inc();
            SLTS_TRUNCATE_RELOCATION_BYTES.add(currentMetadata.getLength());
        }
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
            txn.setExternalCommitStep(() -> chunkedSegmentStorage.getSystemJournal().commitRecord(
                                                SystemJournal.TruncationRecord.builder()
                                                        .segmentName(handle.getSegmentName())
                                                        .offset(offset)
                                                        .firstChunkName(segmentMetadata.getFirstChunk())
                                                        .startOffset(startOffset.get())
                                                        .build()));
        }

        // Finally, commit.
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
            if (null != currentChunkName) {
                Preconditions.checkState(currentMetadata.getName().equals(segmentMetadata.getFirstChunk()),
                        "currentMetadata does not match. Expected = (%s), Actual = (%s)",
                        currentMetadata.getName(), segmentMetadata.getFirstChunk());
                Preconditions.checkState(currentMetadata.getName().equals(currentChunkName),
                        "currentMetadata does not match. Expected = (%s), Actual = (%s)",
                        currentMetadata.getName(), currentChunkName);
            }
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
