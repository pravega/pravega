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
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.ReadIndexBlockMetadata;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import java.time.Duration;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_NUM_CHUNKS_READ;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_READ_BYTES;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_READ_INDEX_BLOCK_LOOKUP_LATENCY;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_READ_INDEX_NUM_SCANNED;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_READ_INDEX_SCAN_LATENCY;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_READ_INSTANT_TPUT;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_READ_LATENCY;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_SYSTEM_NUM_CHUNKS_READ;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_SYSTEM_READ_BYTES;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_SYSTEM_READ_LATENCY;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_SYS_READ_INDEX_BLOCK_LOOKUP_LATENCY;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_SYS_READ_INDEX_NUM_SCANNED;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_SYS_READ_INDEX_SCAN_LATENCY;

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
    private final AtomicInteger cntChunksRead = new AtomicInteger();

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

    @Override
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
        SLTS_NUM_CHUNKS_READ.reportSuccessValue(cntChunksRead.get());
        SLTS_READ_BYTES.add(length);
        if (segmentMetadata.isStorageSystemSegment()) {
            SLTS_SYSTEM_READ_LATENCY.reportSuccessEvent(elapsed);
            SLTS_SYSTEM_NUM_CHUNKS_READ.reportSuccessValue(cntChunksRead.get());
            SLTS_SYSTEM_READ_BYTES.add(length);
        }
        if (elapsed.toMillis() > 0) {
            val bytesPerSecond = 1000L * length / elapsed.toMillis();
            SLTS_READ_INSTANT_TPUT.reportSuccessValue(bytesPerSecond);
        }

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
        val chunkReadFutures = new Vector<CompletableFuture<Void>>();
        return Futures.loop(
                () -> bytesRemaining.get() > 0 && null != currentChunkName,
                () -> {
                    Preconditions.checkState(null != chunkToReadFrom, "chunkToReadFrom is null. currentChunkName=%s Segment=%s", currentChunkName, segmentMetadata.getName());
                    bytesToRead = Math.toIntExact(Math.min(bytesRemaining.get(), chunkToReadFrom.getLength() - (currentOffset.get() - startOffsetForCurrentChunk.get())));
                    if (currentOffset.get() >= startOffsetForCurrentChunk.get() + chunkToReadFrom.getLength()) {
                        // The current chunk is over. Move to the next one.
                        currentChunkName = chunkToReadFrom.getNextChunk();
                        if (null != currentChunkName) {
                            startOffsetForCurrentChunk.addAndGet(chunkToReadFrom.getLength());
                            return txn.get(currentChunkName)
                                    .thenAcceptAsync(storageMetadata -> {
                                        chunkToReadFrom = (ChunkMetadata) storageMetadata;
                                        Preconditions.checkState(null != chunkToReadFrom, "chunkToReadFrom is null. currentChunkName=%s Segment=%s", currentChunkName, segmentMetadata.getName());
                                        log.debug("{} read - reading from next chunk - op={}, segment={}, chunk={}", chunkedSegmentStorage.getLogPrefix(),
                                                System.identityHashCode(this), handle.getSegmentName(), chunkToReadFrom);
                                    }, chunkedSegmentStorage.getExecutor());
                        }
                    } else {
                        Preconditions.checkState(bytesToRead != 0, "bytesToRead is 0. Segment=%s", segmentMetadata.getName());
                        // Read data from the chunk.
                        return CompletableFuture.runAsync(() -> {
                            // Create parallel requests to read each chunk.
                            chunkReadFutures.add(readChunk(chunkToReadFrom.getName(),
                                    currentOffset.get() - startOffsetForCurrentChunk.get(),
                                    bytesToRead,
                                    currentBufferOffset.get()));
                            log.trace("{} read - reading chunk - op={}, segment={}, chunk={} offset={} length={} bufferOffset={}",
                                    chunkedSegmentStorage.getLogPrefix(),
                                    System.identityHashCode(this), handle.getSegmentName(),
                                    chunkToReadFrom.getName(),
                                    currentOffset.get() - startOffsetForCurrentChunk.get(),
                                    bytesToRead,
                                    currentBufferOffset.get());
                            cntChunksRead.incrementAndGet();
                            bytesRemaining.addAndGet(-bytesToRead);
                            currentOffset.addAndGet(bytesToRead);
                            currentBufferOffset.addAndGet(bytesToRead);
                            totalBytesRead.addAndGet(bytesToRead);
                        }, chunkedSegmentStorage.getExecutor());
                    }
                    return CompletableFuture.completedFuture(null);
                }, chunkedSegmentStorage.getExecutor())
                .thenCompose(v -> Futures.allOf(chunkReadFutures));
    }

    private CompletableFuture<Void> readChunk(String chunkName,
                         long fromOffset,
                         int bytesToRead,
                         int bufferOffset) {
        val chunkBytesRemaining = new AtomicInteger(bytesToRead);
        val chunkFromOffset = new AtomicLong(fromOffset);
        val chunkBufferOffset = new AtomicInteger(bufferOffset);
        // Note that technically it is possible that read actually request reads less than requested bytes, requiring additional reads on the same chunk.
        // Hence the for loop below.
        val chunkHandle = ChunkHandle.readHandle(chunkName);
        return Futures.loop(
            () -> chunkBytesRemaining.get() > 0,
            () -> chunkedSegmentStorage.getChunkStorage().read(chunkHandle,
                    chunkFromOffset.get(),
                    chunkBytesRemaining.get(),
                    buffer,
                    chunkBufferOffset.get())
                    .thenAccept(n -> {
                        Preconditions.checkState(n != 0, "Zero bytes read chunk=%s, fromOffset=%d", chunkName, fromOffset);
                        chunkBytesRemaining.addAndGet(-n);
                        chunkFromOffset.addAndGet(n);
                        chunkBufferOffset.addAndGet(n);
                    }),
            chunkedSegmentStorage.getExecutor());
    }

    private CompletableFuture<Void> findChunkForOffset(MetadataTransaction txn) {

        currentChunkName = segmentMetadata.getFirstChunk();
        chunkToReadFrom = null;

        Preconditions.checkState(null != currentChunkName, "currentChunkName must not be null. Segment=%s", segmentMetadata.getName());

        bytesRemaining.set(length);
        currentBufferOffset.set(bufferOffset);
        currentOffset.set(offset);
        totalBytesRead.set(0);

        // Find the first chunk that contains the data.
        startOffsetForCurrentChunk.set(segmentMetadata.getFirstChunkStartOffset());
        boolean shouldOnlyReadLastChunk = offset >= segmentMetadata.getLastChunkStartOffset();
        if (shouldOnlyReadLastChunk) {
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

        final long floorBlockStartOffset = getFloorBlockStartOffset(offset);
        CompletableFuture<Void>  f;
        if (!shouldOnlyReadLastChunk && !segmentMetadata.isStorageSystemSegment() && startOffsetForCurrentChunk.get() < floorBlockStartOffset) {
            val indexLookupTimer = new Timer();
            f = txn.get(NameUtils.getSegmentReadIndexBlockName(segmentMetadata.getName(), floorBlockStartOffset))
                    .thenAcceptAsync(storageMetadata -> {
                        if (null != storageMetadata) {
                            ReadIndexBlockMetadata blockMetadata = (ReadIndexBlockMetadata) storageMetadata;
                            if (blockMetadata.getStartOffset() <= offset) {
                                startOffsetForCurrentChunk.set(blockMetadata.getStartOffset());
                                currentChunkName = blockMetadata.getChunkName();
                                log.debug("{} read - found block index to start scanning - op={}, segment={}, chunk={}, startOffset={}, offset={}.",
                                        chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this),
                                        handle.getSegmentName(), currentChunkName, startOffsetForCurrentChunk.get(), offset);

                                // Note: This just is prefetch call. Do not wait.
                                val nextBlock = getFloorBlockStartOffset(offset + length);
                                if (nextBlock >  floorBlockStartOffset + chunkedSegmentStorage.getConfig().getIndexBlockSize()) {
                                    // We read multiple blocks already
                                    txn.get(NameUtils.getSegmentReadIndexBlockName(segmentMetadata.getName(), nextBlock));
                                } else {
                                    // Prefetch next block index entry.
                                    txn.get(NameUtils.getSegmentReadIndexBlockName(segmentMetadata.getName(), floorBlockStartOffset + chunkedSegmentStorage.getConfig().getIndexBlockSize()));
                                }
                            } else {
                                log.warn("{} read - block entry offset must be floor to requested offset. op={} segment={} offset={} length={} block={}",
                                        chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this),
                                        segmentMetadata, offset, length, blockMetadata);
                            }
                        }
                        if (segmentMetadata.isStorageSystemSegment()) {
                            SLTS_SYS_READ_INDEX_BLOCK_LOOKUP_LATENCY.reportSuccessEvent(indexLookupTimer.getElapsed());
                        } else {
                            SLTS_READ_INDEX_BLOCK_LOOKUP_LATENCY.reportSuccessEvent(indexLookupTimer.getElapsed());
                        }
                    }, chunkedSegmentStorage.getExecutor());
        } else {
           f = CompletableFuture.completedFuture(null);
        }

        val readIndexTimer = new Timer();
        // Navigate to the chunk that contains the first byte of requested data.
        return f.thenComposeAsync( vv -> Futures.loop(
                () -> currentChunkName != null && !isLoopExited,
                () -> txn.get(currentChunkName)
                        .thenAcceptAsync(storageMetadata -> {
                            chunkToReadFrom = (ChunkMetadata) storageMetadata;
                            Preconditions.checkState(null != chunkToReadFrom, "chunkToReadFrom is null. currentChunkName=%s Segment=%s", currentChunkName, segmentMetadata.getName());
                            if (startOffsetForCurrentChunk.get() <= currentOffset.get()
                                    && startOffsetForCurrentChunk.get() + chunkToReadFrom.getLength() > currentOffset.get()) {
                                // we have found a chunk that contains first byte we want to read
                                log.debug("{} read - found chunk to read - op={}, segment={}, chunk={}, startOffset={}, length={}, readOffset={}.",
                                        chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this),
                                        handle.getSegmentName(), chunkToReadFrom, startOffsetForCurrentChunk.get(), chunkToReadFrom.getLength(), currentOffset);
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
                    if (segmentMetadata.isStorageSystemSegment()) {
                        SLTS_SYS_READ_INDEX_SCAN_LATENCY.reportSuccessEvent(elapsed);
                        SLTS_SYS_READ_INDEX_NUM_SCANNED.reportSuccessValue(cntScanned.get());
                    } else {
                        SLTS_READ_INDEX_SCAN_LATENCY.reportSuccessEvent(elapsed);
                        SLTS_READ_INDEX_NUM_SCANNED.reportSuccessValue(cntScanned.get());
                    }

                    // Prefetch possible chunks for next read.
                    if (chunkToReadFrom.getNextChunk() != null) {
                        // Do not wait.
                        txn.get(chunkToReadFrom.getNextChunk());
                    }

                    log.debug("{} read - chunk lookup - op={}, segment={}, offset={}, scanned={}, latency={}.",
                            chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this),
                            handle.getSegmentName(), offset, cntScanned.get(), elapsed.toMillis());
                }, chunkedSegmentStorage.getExecutor()),
        chunkedSegmentStorage.getExecutor());
    }

    private long getFloorBlockStartOffset(long offsetToRead) {
        val floorBlock = offsetToRead / chunkedSegmentStorage.getConfig().getIndexBlockSize();
        val floorBlockStartOffset = floorBlock * chunkedSegmentStorage.getConfig().getIndexBlockSize();
        return floorBlockStartOffset;
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

        if (offset < 0) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    offset, bufferOffset, length, buffer.length));
        }
    }
}
