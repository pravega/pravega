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
import io.pravega.segmentstore.storage.StorageFullException;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadataWritesFencedOutException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_NUM_CHUNKS_ADDED;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_SYSTEM_NUM_CHUNKS_ADDED;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_SYSTEM_WRITE_BYTES;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_SYSTEM_WRITE_LATENCY;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_WRITE_BYTES;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_WRITE_INSTANT_TPUT;
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
    private final List<SystemJournal.SystemJournalRecord> systemLogRecords = new Vector<>();
    private final List<ChunkNameOffsetPair> newReadIndexEntries = new Vector<>();
    private final AtomicInteger chunksAddedCount = new AtomicInteger();

    private volatile boolean isCommitted = false;
    private volatile SegmentMetadata segmentMetadata;
    private volatile boolean isSystemSegment;

    // Check if this is a first write after ownership changed.
    private volatile boolean isFirstWriteAfterFailover;
    private volatile boolean skipOverFailedChunk;

    private final AtomicReference<ChunkMetadata> lastChunkMetadata = new AtomicReference<>(null);
    private volatile ChunkHandle chunkHandle = null;
    private final AtomicInteger bytesRemaining = new AtomicInteger();

    private final AtomicInteger totalBytesRead = new AtomicInteger();

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

    @Override
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
                                                                .thenRunAsync(this::logEnd, chunkedSegmentStorage.getExecutor()),
                                                chunkedSegmentStorage.getExecutor());
                            }, chunkedSegmentStorage.getExecutor());
                }, chunkedSegmentStorage.getExecutor())
                .exceptionally(ex -> (Void) handleException(ex));
    }

    private Object handleException(Throwable e) {
        log.debug("{} write - exception op={}, segment={}, offset={}, length={}.",
                chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, length);
        val ex = Exceptions.unwrap(e);
        if (ex instanceof StorageMetadataWritesFencedOutException) {
            throw new CompletionException(new StorageNotPrimaryException(handle.getSegmentName(), ex));
        }
        if (ex instanceof ChunkStorageFullException) {
            throw new CompletionException(new StorageFullException(handle.getSegmentName(), ex));
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
        SLTS_NUM_CHUNKS_ADDED.reportSuccessValue(chunksAddedCount.get());
        if (segmentMetadata.isStorageSystemSegment()) {
            SLTS_SYSTEM_WRITE_LATENCY.reportSuccessEvent(elapsed);
            SLTS_SYSTEM_WRITE_BYTES.add(length);
            SLTS_SYSTEM_NUM_CHUNKS_ADDED.reportSuccessValue(chunksAddedCount.get());
            chunkedSegmentStorage.reportMetricsForSystemSegment(segmentMetadata);
        }
        if (elapsed.toMillis() > 0) {
            val bytesPerSecond = 1000L * length / elapsed.toMillis();
            SLTS_WRITE_INSTANT_TPUT.reportSuccessValue(bytesPerSecond);
        }
        if (chunkedSegmentStorage.getConfig().getLateWarningThresholdInMillis() < elapsed.toMillis()) {
            log.warn("{} write - late op={}, segment={}, offset={}, length={}, latency={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, length, elapsed.toMillis());
        } else {
            log.debug("{} write - finished op={}, segment={}, offset={}, length={}, latency={}.",
                    chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this), handle.getSegmentName(), offset, length, elapsed.toMillis());
        }
        LoggerHelpers.traceLeave(log, "write", traceId, handle, offset);
    }

    private CompletableFuture<Void> commit(MetadataTransaction txn) {
        // commit all system log records if required.
        if (isSystemSegment && systemLogRecords.size() > 0) {
            // commit all system log records.
            txn.setExternalCommitStep(() -> chunkedSegmentStorage.getSystemJournal().commitRecords(systemLogRecords));
        }
        // Commit
        return txn.commit()
                .thenRunAsync(() -> isCommitted = true, chunkedSegmentStorage.getExecutor());

    }

    private CompletableFuture<Void> writeData(MetadataTransaction txn) {
        val oldChunkCount = segmentMetadata.getChunkCount();
        val oldLength = segmentMetadata.getLength();

        final byte[] expectedContent;
        final InputStream inputStream;
        if (shouldValidateData()) {
            // Read entire input stream at once and save the content for the later use during validation.
            expectedContent = readNBytes(data, length);
            inputStream = new ByteArrayInputStream(expectedContent);
        } else {
            expectedContent = null;
            inputStream = data;
        }

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
                                        inputStream,
                                        chunkHandle,
                                        lastChunkMetadata.get(),
                                        offsetToWriteAt,
                                        writeSize,
                                        expectedContent)
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
                    Preconditions.checkState(totalBytesRead.get() == length, "totalBytesRead (%s) must match length(%s)",
                            totalBytesRead.get(), length);
                    Preconditions.checkState(oldChunkCount + chunksAddedCount.get() == segmentMetadata.getChunkCount(),
                            "Number of chunks do not match. old value (%s) + number of chunks added (%s) must match current chunk count(%s)",
                            oldChunkCount, chunksAddedCount.get(), segmentMetadata.getChunkCount());
                    Preconditions.checkState(oldLength + length == segmentMetadata.getLength(),
                            "New length must match. old value (%s) + length (%s) must match current chunk count(%s)",
                            oldLength, length, segmentMetadata.getLength());
                    if (null != lastChunkMetadata.get()) {
                        Preconditions.checkState(segmentMetadata.getLastChunkStartOffset() + lastChunkMetadata.get().getLength() == segmentMetadata.getLength(),
                                "Last chunk start offset (%s) + Last chunk length (%s) must match segment length (%s)",
                                segmentMetadata.getLastChunkStartOffset(), lastChunkMetadata.get().getLength(), segmentMetadata.getLength());
                    }
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
        String newChunkName = chunkedSegmentStorage.getNewChunkName(handle.getSegmentName(), segmentMetadata.getLength());

        return chunkedSegmentStorage.getGarbageCollector().trackNewChunk(txn.getVersion(), newChunkName)
                .thenComposeAsync( v -> {
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
                                                 int bytesCount,
                                                 byte[] expectedContent) {
        Preconditions.checkState(
            bytesCount > 0, "bytesCount must be positive. Segment=%s Chunk=%s offsetToWriteAt=%s bytesCount=%s",
            segmentMetadata, chunkWrittenMetadata, offsetToWriteAt, bytesCount);
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
                .thenComposeAsync(bytesWritten -> {
                    // Validate.
                    Preconditions.checkState(bytesWritten == bytesCount,
                            "bytesWritten (%s) must equal bytesCount(%s). Segment=%s Chunk=%s offsetToWriteAt=%s",
                            bytesWritten, bytesCount, segmentMetadata, chunkWrittenMetadata, offsetToWriteAt);

                    CompletableFuture<Void> validation = CompletableFuture.completedFuture(null);
                    if (chunkedSegmentStorage.getConfig().isSelfCheckForMetadataEnabled()) {
                        validation = validation.thenComposeAsync(v ->
                                chunkedSegmentStorage.getChunkStorage().getInfo(chunkHandle.getChunkName())
                                    .thenAcceptAsync( chunkInfo -> {
                                        Preconditions.checkState(chunkInfo.getLength() == (chunkWrittenMetadata.getLength() + bytesWritten),
                                                "Length of chunk does not match expected length. Expected (%s) Actual (%s)",
                                                chunkWrittenMetadata.getLength() + bytesWritten, chunkInfo.getLength());
                                    }, chunkedSegmentStorage.getExecutor()), chunkedSegmentStorage.getExecutor());
                    }

                    if (shouldValidateData()) {
                        validation = validation.thenComposeAsync(v ->
                                validateWrittenData(chunkHandle, offsetToWriteAt, totalBytesRead.get(), bytesCount, expectedContent),
                                chunkedSegmentStorage.getExecutor());
                    }
                    return validation
                            .thenApplyAsync( v -> bytesWritten, chunkedSegmentStorage.getExecutor());
                }, chunkedSegmentStorage.getExecutor())
                .thenAcceptAsync(bytesWritten -> {
                    // Update the metadata for segment and chunk.
                    segmentMetadata.setLength(segmentMetadata.getLength() + bytesWritten);
                    chunkWrittenMetadata.setLength(chunkWrittenMetadata.getLength() + bytesWritten);
                    txn.update(chunkWrittenMetadata);
                    txn.update(segmentMetadata);

                    // Update iteration state
                    bytesRemaining.addAndGet(-bytesWritten);
                    currentOffset.addAndGet(bytesWritten);
                    totalBytesRead.addAndGet(bytesWritten);
                    // Add system journal record for append.
                    if (isSystemSegment) {
                        systemLogRecords.add(
                                SystemJournal.AppendRecord.builder()
                                        .segmentName(segmentMetadata.getName())
                                        .chunkName(chunkWrittenMetadata.getName())
                                        .offset(offsetToWriteAt)
                                        .length(bytesWritten)
                                        .build());
                    }
                }, chunkedSegmentStorage.getExecutor())
                .handleAsync((v, e) -> {
                    if (null != e) {
                        val ex = Exceptions.unwrap(e);
                        if (ex instanceof InvalidOffsetException) {
                            val invalidEx = (InvalidOffsetException) ex;
                            // if the length of chunk on the LTS is greater, then just skip this chunk.
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

    private CompletableFuture<Void> validateWrittenData(ChunkHandle chunkHandle, long startOffsetInChunk, int startOffestInInputData, int bytesCount, byte[] expectedContent) {
        val bufferForRead = new byte[bytesCount];
        return readChunk(chunkHandle, startOffsetInChunk, bytesCount, bufferForRead)
                .thenAcceptAsync(v -> {
                    val mismatch = Arrays.mismatch(bufferForRead, 0, bytesCount,
                            expectedContent, startOffestInInputData, startOffestInInputData + bytesCount);
                        Preconditions.checkState(-1 == mismatch, "Data read from chunk differs from data written at offset %s",
                                startOffsetInChunk + mismatch );
                }, chunkedSegmentStorage.getExecutor());
    }

    private CompletableFuture<Void> readChunk(ChunkHandle chunkHandle,
                                              long fromOffset,
                                              int bytesToRead,
                                              byte[] buffer) {
        val chunkBytesRemaining = new AtomicInteger(bytesToRead);
        val chunkFromOffset = new AtomicLong(fromOffset);
        val chunkBufferOffset = new AtomicInteger(0);
        return Futures.loop(
                () -> chunkBytesRemaining.get() > 0,
                () -> chunkedSegmentStorage.getChunkStorage().read(chunkHandle,
                                chunkFromOffset.get(),
                                chunkBytesRemaining.get(),
                                buffer,
                                chunkBufferOffset.get())
                        .thenAccept(n -> {
                            Preconditions.checkState(n != 0, "Zero bytes read chunk=%s, fromOffset=%d", chunkHandle.getChunkName(), fromOffset);
                            chunkBytesRemaining.addAndGet(-n);
                            chunkFromOffset.addAndGet(n);
                            chunkBufferOffset.addAndGet(n);
                        }),
                chunkedSegmentStorage.getExecutor());
    }

    @SneakyThrows
    private static byte[] readNBytes(InputStream data, int bytesCount) {
        return data.readNBytes(bytesCount);
    }

    private boolean shouldValidateData() {
        return chunkedSegmentStorage.getConfig().isSelfCheckForDataEnabled() && chunkedSegmentStorage.getChunkStorage().supportsDataIntegrityCheck();
    }

}
