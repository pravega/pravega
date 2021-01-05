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

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Defragments the list of chunks for a given segment.
 * It finds eligible consecutive chunks that can be merged together.
 * The sublist of such eligible chunks is replaced with single new large chunk.
 * Conceptually this is like deleting nodes from middle of the list of chunks and replacing them with one or more nodes.
 * <ul>
 * <Ul>
 * <li> In the absence of defragmentation, the number of chunks for individual segments keeps on increasing.
 * When we have too many small chunks (say because many transactions with little data on some segments), the segment
 * is fragmented - this may impact both the read throughput and the performance of the metadata store.
 * This problem is further intensified when we have stores that do not support append semantics (e.g., non-extended S3) and
 * each write becomes a separate chunk.
 * </li>
 * <li>
 * If the underlying storage provides some facility to stitch together smaller chunk into larger chunks, then we do
 * actually want to exploit that, specially when the underlying implementation is only a metadata operation. We want
 * to leverage multi-part uploads in object stores that support it (e.g., AWS S3, Dell EMC ECS) as they are typically
 * only metadata operations, reducing the overall cost of the merging them together. HDFS also supports merges,
 * whereas NFS has no concept of merging natively.
 *
 * As chunks become larger, append writes (read source completely and append it back at the end of target)
 * become inefficient. Consequently, a native option for merging is desirable. We use such native merge capability
 * when available, and if not available, then we use appends.
 * </li>
 * <li>
 * Ideally we want the defrag to be run in the background periodically and not on the write/concat path.
 * We can then fine tune that background task to run optimally with low overhead.
 * We might be able to give more knobs to tune its parameters (Eg. threshold on number of chunks).
 * </li>
 * <li>
 * Defrag operation will respect max rolling size and will not create chunks greater than that size.
 * </li>
 * </ul>
 * <ul>
 * What controls whether we invoke concat or simulate through appends?
 * There are a few different capabilities that ChunkStorage needs to provide.
 *
 * <li>Does ChunkStorage support appending to existing chunks? For non-extended S3 compatible this would return false.
 * This is indicated by supportsAppend.</li>
 * <li>Does ChunkStorage support for concatenating chunks ? This is indicated by supportsConcat.
 * If this is true then concat operation will be invoked otherwise chunks will be appended.</li>
 * <li>There are some obvious constraints - For ChunkStorage support any concat functionality it must support either
 * append or concat.</li>
 * <li>Also when ChunkStorage supports both concat and append, ChunkedSegmentStorage will invoke appropriate method
 * depending on size of target and source chunks. (Eg. ECS)</li>
 * </ul>
 *
 * <ul>
 * What controls defrag?
 * Following are two additional parameters that control while concatenating.
 * <li>minSizeLimitForConcat: Size of chunk in bytes above which it is no longer considered a small object.
 * For small source objects, append is used instead of using concat. (For really small txn it is rather efficient to use append than MPU).</li>
 * <li>maxSizeLimitForConcat: Size of chunk in bytes above which it is no longer considered for concat. (Eg S3 might have max limit on chunk size).</li>
 * In short there is a size beyond which using append is not advisable. Conversely there is a size below which concat is not efficient.(minSizeLimitForConcat )
 * Then there is limit which concatenating does not make sense maxSizeLimitForConcat
 * </li>
 * <li>
 * What is the defrag algorithm
 * <pre>
 * While(segment.hasConcatableChunks()){
 *     Set<List<Chunk>> s = FindConsecutiveConcatableChunks();
 *     For (List<chunk> list : s){
 *        ConcatChunks (list);
 *     }
 * }
 * </pre>
 * </li>
 * </ul>
 */
@Slf4j
class DefragmentOperation implements Callable<CompletableFuture<Void>> {
    private final MetadataTransaction txn;
    private final SegmentMetadata segmentMetadata;
    private final String startChunkName;
    private final String lastChunkName;
    private final ArrayList<String> chunksToDelete;
    private final ChunkedSegmentStorage chunkedSegmentStorage;

    private volatile ArrayList<ChunkInfo> chunksToConcat = new ArrayList<>();
    private final List<ChunkNameOffsetPair> newReadIndexEntries;
    private volatile ChunkMetadata target;
    private volatile String targetChunkName;
    private final AtomicBoolean useAppend = new AtomicBoolean();
    private final AtomicBoolean skipFailed = new AtomicBoolean();
    private final AtomicLong targetSizeAfterConcat = new AtomicLong();
    private volatile String nextChunkName;
    private volatile ChunkMetadata next = null;

    private final AtomicLong  writeAtOffset = new AtomicLong();
    private final AtomicInteger readAtOffset = new AtomicInteger();
    private final AtomicInteger bytesToRead = new AtomicInteger();
    private final AtomicInteger currentArgIndex = new AtomicInteger();
    private final AtomicLong currentIndexOffset = new AtomicLong();

    DefragmentOperation(ChunkedSegmentStorage chunkedSegmentStorage,
                        MetadataTransaction txn,
                        SegmentMetadata segmentMetadata,
                        String startChunkName,
                        String lastChunkName,
                        ArrayList<String> chunksToDelete,
                        List<ChunkNameOffsetPair> newReadIndexEntries,
                        long currentIndexOffset) {
        this.txn = txn;
        this.segmentMetadata = segmentMetadata;
        this.startChunkName = startChunkName;
        this.lastChunkName = lastChunkName;
        this.chunksToDelete = chunksToDelete;
        this.newReadIndexEntries = newReadIndexEntries;
        this.chunkedSegmentStorage = chunkedSegmentStorage;
        this.currentIndexOffset.set(currentIndexOffset);
    }

    public CompletableFuture<Void> call() {
        // The algorithm is actually very simple.
        // It tries to concat all small chunks using appends first.
        // Then it tries to concat remaining chunks using concat if available.
        // To implement it using single loop we toggle between concat with append and concat modes. (Instead of two passes.)
        useAppend.set(true);
        targetChunkName = startChunkName;

        // Iterate through chunk list
        // Make sure no invariants are broken.
        return Futures.loop(
                () -> null != targetChunkName && !targetChunkName.equals(lastChunkName),
                () -> gatherChunks()
                        .thenComposeAsync(v -> {
                            // Note - After above while loop is exited nextChunkName points to chunk next to last one to be concat.
                            // Which means target should now point to it as next after concat is complete.

                            // If there are chunks that can be appended together then concat them.
                            CompletableFuture<Void> f;
                            if (chunksToConcat.size() > 1) {
                                // Concat
                                f = concatChunks()
                                .handleAsync((vv, ex) -> {
                                    if (null != ex) {
                                        ex = Exceptions.unwrap(ex);
                                        if (ex instanceof InvalidOffsetException) {
                                            val invalidEx = (InvalidOffsetException) ex;
                                            if (invalidEx.getExpectedOffset() > invalidEx.getGivenOffset()) {
                                                // Skip ahead by 1 chunk.
                                                targetChunkName = chunksToConcat.get(1).getName();
                                                chunksToConcat.clear();
                                                skipFailed.set(true);
                                                log.debug("{} defrag - skipping partially written chunk op={}, {}",
                                                        chunkedSegmentStorage.getLogPrefix(), System.identityHashCode(this),
                                                        invalidEx.getMessage());
                                                return null;
                                            }
                                        }
                                        throw new CompletionException(ex);
                                    }
                                    return vv;
                                }, chunkedSegmentStorage.getExecutor());
                            } else {
                                f = CompletableFuture.completedFuture(null);
                            }
                            return f.thenRunAsync(() -> {
                                if (skipFailed.compareAndSet(true, false)) {
                                    return;
                                }
                                // Move on to next place in list where we can concat if we are done with append based concatenations.
                                if (!useAppend.get()) {
                                    targetChunkName = nextChunkName;
                                }
                                // Toggle
                                useAppend.set(!useAppend.get());
                            }, chunkedSegmentStorage.getExecutor());
                        }, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor())
                .thenRunAsync(segmentMetadata::checkInvariants, chunkedSegmentStorage.getExecutor())
                .thenComposeAsync(vvv -> updateReadIndex(), chunkedSegmentStorage.getExecutor());
    }

    private CompletableFuture<Void> concatChunks() {
        val concatArgs = new ConcatArgument[chunksToConcat.size()];
        for (int i = 0; i < chunksToConcat.size(); i++) {
            concatArgs[i] = ConcatArgument.fromChunkInfo(chunksToConcat.get(i));
        }
        final CompletableFuture<Integer> f;
        if ((!useAppend.get() && chunkedSegmentStorage.getChunkStorage().supportsConcat()) || !chunkedSegmentStorage.shouldAppend()) {
            f = chunkedSegmentStorage.getChunkStorage().concat(concatArgs);
        } else {
            f = concatUsingAppend(concatArgs);
        }

        return f.thenComposeAsync(v -> {
            // Delete chunks.
            for (int i = 1; i < chunksToConcat.size(); i++) {
                chunksToDelete.add(chunksToConcat.get(i).getName());
            }

            // Set the pointers
            target.setLength(targetSizeAfterConcat.get());
            target.setNextChunk(nextChunkName);

            // If target is the last chunk after this then update metadata accordingly
            if (null == nextChunkName) {
                segmentMetadata.setLastChunk(target.getName());
                segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength() - target.getLength());
            }

            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
            // Update metadata for affected chunks.
            for (int i = 1; i < concatArgs.length; i++) {
                final int n = i;
                futures.add(txn.get(concatArgs[n].getName())
                                .thenAcceptAsync(metadata -> {
                                    ((ChunkMetadata) metadata).setActive(false);
                                    txn.update(metadata);
                                }, chunkedSegmentStorage.getExecutor()));
                segmentMetadata.decrementChunkCount();
            }
            return Futures.allOf(futures).thenRunAsync(() -> {
                txn.update(target);
                txn.update(segmentMetadata);
            }, chunkedSegmentStorage.getExecutor());

        }, chunkedSegmentStorage.getExecutor());

    }

    private CompletableFuture<Void> gatherChunks() {
        return txn.get(targetChunkName)
                .thenComposeAsync(storageMetadata -> {
                    target = (ChunkMetadata) storageMetadata;
                    chunksToConcat = new ArrayList<>();
                    targetSizeAfterConcat.set(target.getLength());

                    // Add target to the list of chunks
                    chunksToConcat.add(new ChunkInfo(targetSizeAfterConcat.get(), targetChunkName));

                    nextChunkName = target.getNextChunk();
                    return txn.get(nextChunkName)
                            .thenComposeAsync(storageMetadata1 -> {

                                next = (ChunkMetadata) storageMetadata1;
                                // Gather list of chunks that can be appended together.
                                return Futures.loop(
                                        () ->
                                                null != nextChunkName
                                                        && !(useAppend.get() && chunkedSegmentStorage.getConfig().getMinSizeLimitForConcat() < next.getLength())
                                                        && !(targetSizeAfterConcat.get() + next.getLength() > segmentMetadata.getMaxRollinglength() || next.getLength() > chunkedSegmentStorage.getConfig().getMaxSizeLimitForConcat()),
                                        () -> txn.get(nextChunkName)
                                                .thenAcceptAsync(storageMetadata2 -> {
                                                    next = (ChunkMetadata) storageMetadata2;
                                                    chunksToConcat.add(new ChunkInfo(next.getLength(), nextChunkName));
                                                    targetSizeAfterConcat.addAndGet(next.getLength());

                                                    nextChunkName = next.getNextChunk();
                                                }, chunkedSegmentStorage.getExecutor()),
                                        chunkedSegmentStorage.getExecutor());

                            }, chunkedSegmentStorage.getExecutor());
                }, chunkedSegmentStorage.getExecutor());
    }

    private CompletableFuture<Integer> concatUsingAppend(ConcatArgument[] concatArgs) {
        writeAtOffset.set(concatArgs[0].getLength());
        val writeHandle = ChunkHandle.writeHandle(concatArgs[0].getName());
        currentArgIndex.set(1);
        return Futures.loop(() -> currentArgIndex.get() < concatArgs.length,
                () -> {
                    readAtOffset.set(0);
                    val arg = concatArgs[currentArgIndex.get()];
                    bytesToRead.set(Math.toIntExact(arg.getLength()));

                    return copyBytes(writeHandle, arg)
                            .thenRunAsync(currentArgIndex::incrementAndGet, chunkedSegmentStorage.getExecutor());
                },
                chunkedSegmentStorage.getExecutor())
                .thenApplyAsync(v -> 0, chunkedSegmentStorage.getExecutor());
    }

    private CompletableFuture<Void> copyBytes(ChunkHandle writeHandle, ConcatArgument arg) {
        return Futures.loop(
                () -> bytesToRead.get() > 0,
                () -> {
                    val buffer = new byte[Math.min(chunkedSegmentStorage.getConfig().getMaxBufferSizeForChunkDataTransfer(), bytesToRead.get())];
                    return chunkedSegmentStorage.getChunkStorage().read(ChunkHandle.readHandle(arg.getName()), readAtOffset.get(), buffer.length, buffer, 0)
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

    private CompletableFuture<Void> updateReadIndex() {
        return new ChunkIterator(chunkedSegmentStorage, txn, segmentMetadata, startChunkName, lastChunkName)
                .forEach((metadata, name) -> {
                    newReadIndexEntries.add(ChunkNameOffsetPair.builder()
                            .chunkName(name)
                            .offset(currentIndexOffset.get())
                            .build());
                    currentIndexOffset.addAndGet(metadata.getLength());
                })
                .thenRunAsync(() -> {
                    if (newReadIndexEntries.size() > 0) {
                        val start = newReadIndexEntries.get(0).getOffset();
                        val end = currentIndexOffset.get();
                        chunkedSegmentStorage.addBlockIndexEntries(txn, segmentMetadata.getName(), start, end, newReadIndexEntries);
                    }
                }, chunkedSegmentStorage.getExecutor());
    }
}
