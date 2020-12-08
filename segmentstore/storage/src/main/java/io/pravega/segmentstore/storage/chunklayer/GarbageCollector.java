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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements simple garbage collector for cleaning up the deleted chunks.
 */
@Slf4j
public class GarbageCollector implements AutoCloseable {
    /**
     * Set of garbage chunks.
     */
    @Getter
    private final ConcurrentHashMap<String, Long> garbageChunks = new ConcurrentHashMap<>();

    private final ChunkedSegmentStorage chunkedSegmentStorage;

    private final ChunkedSegmentStorageConfig config;

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicBoolean suspended = new AtomicBoolean();

    private final long delayInMillis;

    private CompletableFuture<Void> loopFuture;

    /**
     * Constructs a new instance.
     *
     * @param chunkedSegmentStorage Instance of {@link ChunkedSegmentStorage}.
     * @param config Configuration to use.
     */
    public GarbageCollector(ChunkedSegmentStorage chunkedSegmentStorage, ChunkedSegmentStorageConfig config) {
        this.chunkedSegmentStorage = chunkedSegmentStorage;
        this.config = config;
        this.delayInMillis = Duration.ofSeconds(config.getGarbageCollectionFrequencyInSeconds()).toMillis();
    }

    /**
     * Initializes this instance.
     */
    public void initialize() {
        loopFuture = run();
    }

    public CompletableFuture<Void> run() {
        return Futures.loop(
                () -> !closed.get(),
                () -> CompletableFuture.completedFuture(null)
                        .thenComposeAsync(v -> deleteGarbage(true, config.getGarbageCollectionConcurrency()),
                                chunkedSegmentStorage.getExecutor())
                        .handleAsync((v, ex) -> {
                            if (null != ex) {
                                log.error("{} Error during run {}.", chunkedSegmentStorage.getLogPrefix(), ex);
                            }
                            return null;
                        }), chunkedSegmentStorage.getExecutor());
    }

    /**
     * Sets whether backgound claenup is suspended or not.
     * @param value
     */
    void setSuspended(boolean value) {
        suspended.set(value);
    }

    /**
     * collect the garbage chunks.
     *
     * @param chunksToDelete List of chunks to delete.
     */
    CompletableFuture<Void> addToGarbage(Collection<String> chunksToDelete) {
        val currentTime = System.currentTimeMillis();
        chunksToDelete.forEach(chunkToDelete -> garbageChunks.put(chunkToDelete, currentTime));
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Delete the garbage chunks.
     *
     * @param isBackground True if the caller is backgound task else False if called explicitly.
     * @param maxItems Maximum number of items to delete at a time.
     * @return CompletableFuture which is completed when garbage is deleted.
     */
    CompletableFuture<Void> deleteGarbage(boolean isBackground, int maxItems) {
        if (suspended.get() && isBackground) {
            return CompletableFuture.completedFuture(null);
        }
        val currentTime = System.currentTimeMillis();
        val chunksToDelete = new ArrayList<String>();
        int count = 0;
        for (val entry : garbageChunks.entrySet()) {
            if (canDelete(entry.getValue(), currentTime)) {
                chunksToDelete.add(entry.getKey());
                // Throttle how many chunks are deleted at once.
                if (++count >= maxItems) {
                    break;
                }
            }
        }

        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (val chunkToDelete : chunksToDelete) {
            val txn = chunkedSegmentStorage.getMetadataStore().beginTransaction(false, chunkToDelete);
            val future =
                    txn.get(chunkToDelete)
                    .thenComposeAsync( metadata -> {
                            val chunkMetadata = (ChunkMetadata) metadata;
                            // Check whether the chunk is marked as inactive
                            if (!chunkMetadata.isActive()) {
                                return chunkedSegmentStorage.getChunkStorage().openWrite(chunkToDelete)
                                        .thenComposeAsync(chunkedSegmentStorage.getChunkStorage()::delete, chunkedSegmentStorage.getExecutor())
                                        .thenRunAsync(() -> {
                                            garbageChunks.remove(chunkToDelete);
                                            txn.delete(chunkToDelete);
                                            log.debug("{} deleteGarbage - deleted chunk={}.", chunkedSegmentStorage.getLogPrefix(), chunkToDelete);
                                        }, chunkedSegmentStorage.getExecutor())
                                        .thenComposeAsync(v -> txn.commit(), chunkedSegmentStorage.getExecutor())
                                        .exceptionally(e -> {
                                            val ex = Exceptions.unwrap(e);
                                            if (ex instanceof ChunkNotFoundException) {
                                                log.debug("{} deleteGarbage - Could not delete garbage chunk {}.", chunkedSegmentStorage.getLogPrefix(), chunkToDelete);
                                            } else {
                                                log.warn("{} deleteGarbage - Could not delete garbage chunk {}.", chunkedSegmentStorage.getLogPrefix(), chunkToDelete);
                                            }
                                            return null;
                                        });
                            } else {
                                log.info("{} deleteGarbage - Chunk is not marked as garbage{}.", chunkedSegmentStorage.getLogPrefix(), chunkToDelete);
                                return CompletableFuture.completedFuture(null);
                            }
                        })
                    .whenCompleteAsync((v, ex) -> {
                        txn.close();
                    });
                    futures.add(future);
        }
        return Futures.allOf(futures);
    }

    /**
     * Marks given chunk as not garbage.
     *
     * @param chunkName Name of the chunk.
     */
    void removeFromGarbage(String chunkName) {
        garbageChunks.remove(chunkName);
    }

    boolean canDelete(long time, long currentTime) {
        return delayInMillis < (currentTime - time);
    }

    @Override
    public void close() throws Exception {
        closed.set(true);
        loopFuture.cancel(true);
    }
}