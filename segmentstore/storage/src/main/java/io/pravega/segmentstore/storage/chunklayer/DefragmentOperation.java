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

import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import lombok.val;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Defragments the list of chunks for a given segment.
 * It finds eligible consecutive chunks that can be merged together.
 * The sublist such elgible chunks is replaced with single new chunk record corresponding to new large chunk.
 * Conceptually this is like deleting nodes from middle of the list of chunks.
 *
 * <Ul>
 * <li> In the absence of defragmentation, the number of chunks for individual segments keeps on increasing.
 * When we have too many small chunks (say because many transactions with little data on some segments), the segment
 * is fragmented - this may impact both the read throughput and the performance of the metadata store.
 * This problem is further intensified when we have stores that do not support append semantics (e.g., stock S3) and
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
 * <li>
 * Defrag operation will respect max rolling size and will not create chunks greater than that size.
 * </li>
 * </ul>
 *
 * What controls whether we invoke concat or simulate through appends?
 * There are a few different capabilities that ChunkStorage needs to provide.
 * <ul>
 * <li>Does ChunkStorage support appending to existing chunks? For vanilla S3 compatible this would return false.
 * This is indicated by supportsAppend.</li>
 * <li>Does ChunkStorage support for concatenating chunks ? This is indicated by supportsConcat.
 * If this is true then concat operation will be invoked otherwise chunks will be appended.</li>
 * <li>There are some obvious constraints - For ChunkStorage support any concat functionality it must support either
 * append or concat.</li>
 * <li>Also when ChunkStorage supports both concat and append, ChunkedSegmentStorage will invoke appropriate method
 * depending on size of target and source chunks. (Eg. ECS)</li>
 * </ul>
 *
 * <li>
 * What controls defrag?
 * There are two additional parameters that control when concat
 * <li>minSizeLimitForConcat: Size of chunk in bytes above which it is no longer considered a small object.
 * For small source objects, append is used instead of using concat. (For really small txn it is rather efficient to use append than MPU).</li>
 * <li>maxSizeLimitForConcat: Size of chunk in bytes above which it is no longer considered for concat. (Eg S3 might have max limit on chunk size).</li>
 * In short there is a size beyond which using append is not advisable. Conversely there is a size below which concat is not efficient.(minSizeLimitForConcat )
 * Then there is limit which concating does not make sense maxSizeLimitForConcat
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
class DefragmentOperation implements Callable<CompletableFuture<Void>> {
    private final MetadataTransaction txn;
    private final SegmentMetadata segmentMetadata;
    private final String startChunkName;
    private final String lastChunkName;
    private final ArrayList<String> chunksToDelete;
    private final ChunkedSegmentStorage chunkedSegmentStorage;

    private volatile ArrayList<ChunkInfo> chunksToConcat = new ArrayList<>();

    private volatile ChunkMetadata target;
    private volatile String targetChunkName;
    private volatile boolean useAppend;
    private volatile long targetSizeAfterConcat;
    private volatile String nextChunkName;
    private volatile ChunkMetadata next = null;

    private volatile long writeAtOffset;
    private volatile int readAtOffset = 0;
    private volatile int bytesToRead;
    private final AtomicInteger currentArgIndex = new AtomicInteger();

    DefragmentOperation(ChunkedSegmentStorage chunkedSegmentStorage, MetadataTransaction txn, SegmentMetadata segmentMetadata, String startChunkName, String lastChunkName, ArrayList<String> chunksToDelete) {
        this.txn = txn;
        this.segmentMetadata = segmentMetadata;
        this.startChunkName = startChunkName;
        this.lastChunkName = lastChunkName;
        this.chunksToDelete = chunksToDelete;
        this.chunkedSegmentStorage = chunkedSegmentStorage;
    }

    public CompletableFuture<Void> call() {
        // The algorithm is actually very simple.
        // It tries to concat all small chunks using appends first.
        // Then it tries to concat remaining chunks using concat if available.
        // To implement it using single loop we toggle between concat with append and concat modes. (Instead of two passes.)
        useAppend = true;
        targetChunkName = startChunkName;

        // Iterate through chunk list
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
                                f = concatChunks();
                            } else {
                                f = CompletableFuture.completedFuture(null);
                            }
                            return f.thenApplyAsync(vv -> {
                                // Move on to next place in list where we can concat if we are done with append based concats.
                                if (!useAppend) {
                                    targetChunkName = nextChunkName;
                                }
                                // Toggle
                                useAppend = !useAppend;
                                return null;
                            }, chunkedSegmentStorage.getExecutor());
                        }, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor())
                .thenApplyAsync(vv -> {
                    // Make sure no invariants are broken.
                    segmentMetadata.checkInvariants();
                    return null;
                }, chunkedSegmentStorage.getExecutor());
    }

    private CompletableFuture<Void> concatChunks() {
        val concatArgs = new ConcatArgument[chunksToConcat.size()];
        for (int i = 0; i < chunksToConcat.size(); i++) {
            concatArgs[i] = ConcatArgument.fromChunkInfo(chunksToConcat.get(i));
        }
        final CompletableFuture<Integer> f;
        if (!useAppend && chunkedSegmentStorage.getChunkStorage().supportsConcat()) {
            f = chunkedSegmentStorage.getChunkStorage().concat(concatArgs);
        } else {
            f = concatUsingAppend(concatArgs);
        }

        return f.thenApplyAsync(v -> {
            // Delete chunks.
            for (int i = 1; i < chunksToConcat.size(); i++) {
                chunksToDelete.add(chunksToConcat.get(i).getName());
            }

            // Set the pointers
            target.setLength(targetSizeAfterConcat);
            target.setNextChunk(nextChunkName);

            // If target is the last chunk after this then update metadata accordingly
            if (null == nextChunkName) {
                segmentMetadata.setLastChunk(target.getName());
                segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength() - target.getLength());
            }

            // Update metadata for affected chunks.
            for (int i = 1; i < concatArgs.length; i++) {
                txn.delete(concatArgs[i].getName());
                segmentMetadata.decrementChunkCount();
            }
            txn.update(target);
            txn.update(segmentMetadata);
            return null;
        }, chunkedSegmentStorage.getExecutor());

    }

    private CompletableFuture<Void> gatherChunks() {
        return txn.get(targetChunkName)
                .thenComposeAsync(storageMetadata -> {
                    target = (ChunkMetadata) storageMetadata;
                    chunksToConcat = new ArrayList<>();
                    targetSizeAfterConcat = target.getLength();

                    // Add target to the list of chunks
                    chunksToConcat.add(new ChunkInfo(targetSizeAfterConcat, targetChunkName));

                    nextChunkName = target.getNextChunk();
                    return txn.get(nextChunkName)
                            .thenComposeAsync(storageMetadata1 -> {

                                next = (ChunkMetadata) storageMetadata1;
                                // Gather list of chunks that can be appended together.
                                return Futures.loop(
                                        () ->
                                                null != nextChunkName
                                                        && !(useAppend && chunkedSegmentStorage.getConfig().getMinSizeLimitForConcat() < next.getLength())
                                                        && !(targetSizeAfterConcat + next.getLength() > segmentMetadata.getMaxRollinglength() || next.getLength() > chunkedSegmentStorage.getConfig().getMaxSizeLimitForConcat()),
                                        () -> txn.get(nextChunkName)
                                                .thenApplyAsync(storageMetadata2 -> {
                                                    next = (ChunkMetadata) storageMetadata2;
                                                    chunksToConcat.add(new ChunkInfo(next.getLength(), nextChunkName));
                                                    targetSizeAfterConcat += next.getLength();

                                                    nextChunkName = next.getNextChunk();
                                                    return null;
                                                }, chunkedSegmentStorage.getExecutor()),
                                        chunkedSegmentStorage.getExecutor());

                            }, chunkedSegmentStorage.getExecutor());
                }, chunkedSegmentStorage.getExecutor());
    }

    private CompletableFuture<Integer> concatUsingAppend(ConcatArgument[] concatArgs) {
        writeAtOffset = concatArgs[0].getLength();
        val writeHandle = ChunkHandle.writeHandle(concatArgs[0].getName());
        currentArgIndex.set(1);
        return Futures.loop(() -> currentArgIndex.get() < concatArgs.length,
                () -> {
                    readAtOffset = 0;
                    val arg = concatArgs[currentArgIndex.get()];
                    bytesToRead = Math.toIntExact(arg.getLength());

                    return copyBytes(writeHandle, arg)
                            .thenApplyAsync(v -> {
                                currentArgIndex.incrementAndGet();
                                return null;
                            }, chunkedSegmentStorage.getExecutor());
                },
                chunkedSegmentStorage.getExecutor())
                .thenApplyAsync(v -> 0, chunkedSegmentStorage.getExecutor());
    }

    private CompletableFuture<Void> copyBytes(ChunkHandle writeHandle, ConcatArgument arg) {
        return Futures.loop(
                () -> bytesToRead > 0,
                () -> {
                    val buffer = new byte[Math.min(chunkedSegmentStorage.getConfig().getMaxBufferSizeForChunkDataTransfer(), bytesToRead)];
                    return chunkedSegmentStorage.getChunkStorage().read(ChunkHandle.readHandle(arg.getName()), readAtOffset, buffer.length, buffer, 0)
                            .thenComposeAsync(size -> {
                                bytesToRead -= size;
                                readAtOffset += size;
                                return chunkedSegmentStorage.getChunkStorage().write(writeHandle, writeAtOffset, size, new ByteArrayInputStream(buffer, 0, size))
                                        .thenApplyAsync(written -> {
                                            writeAtOffset += written;
                                            return null;
                                        }, chunkedSegmentStorage.getExecutor());
                            }, chunkedSegmentStorage.getExecutor());
                },
                chunkedSegmentStorage.getExecutor()
        );
    }
}
