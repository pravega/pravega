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
import io.pravega.common.concurrent.AbstractThreadPoolService;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements simple garbage collector for cleaning up the deleted chunks.
 * The garbage collector maintains a in memory queue of chunks to delete which is drained by a background task.
 * This queue is populated by following
 * 1. Various SLTS operations requesting deletes
 * 2. Background task that scans all records to find undeleted chunks inside metadata (not yet implemented).
 * 3. Background task that scans all LTS to find unaccounted chunks that are deemed garbage (not yet implemented).
 *
 * The background task throttles itself in two ways.
 * 1. It limits number of concurrent deletes at a time, so that it doesn't interfere with foreground SLTS tasks.
 * 2. It limits the number of items in the queue.
 */
@Slf4j
public class GarbageCollector extends AbstractThreadPoolService implements AutoCloseable {
    /**
     * Set of garbage chunks.
     * This queue needs to be lock free, hence ConcurrentLinkedQueue.
     */
    @Getter
    private final ConcurrentLinkedQueue<GarbageChunkInfo> garbageChunks = new ConcurrentLinkedQueue<>();

    private final ChunkedSegmentStorage chunkedSegmentStorage;

    private final ChunkedSegmentStorageConfig config;

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicBoolean suspended = new AtomicBoolean();

    /**
     * Keeps track of queue size.
     * Size is an expensive operation on ConcurrentLinkedQueue.
     */
    private final AtomicInteger queueSize = new AtomicInteger();

    private CompletableFuture<Void> loopFuture;

    /**
     * Constructs a new instance.
     *
     * @param chunkedSegmentStorage Instance of {@link ChunkedSegmentStorage}.
     * @param config Configuration to use.
     */
    public GarbageCollector(ChunkedSegmentStorage chunkedSegmentStorage, ChunkedSegmentStorageConfig config) {
        super("io.pravega.segmentstore.storage.chunklayer.GarbageCollector", (ScheduledExecutorService) chunkedSegmentStorage.getExecutor());
        this.chunkedSegmentStorage = Preconditions.checkNotNull(chunkedSegmentStorage, "chunkedSegmentStorage");
        this.config = Preconditions.checkNotNull(config, "config");
    }

    /**
     * Initializes this instance.
     */
    public void initialize() {
        Services.startAsync(this, this.executor);
    }

    /**
     * Gets a value indicating how much to wait for the service to shut down, before failing it.
     *
     * @return The Duration.
     */
    @Override
    protected Duration getShutdownTimeout() {
        return null;
    }

    /**
     * Main execution of the Service. When this Future completes, the service auto-shuts down.
     *
     * @return A CompletableFuture that, when completed, indicates the service is terminated. If the Future completed
     * exceptionally, the Service will shut down with failure, otherwise it will terminate normally.
     */
    @Override
    protected CompletableFuture<Void> doRun() {
        loopFuture = Futures.loop(
                this::canRun,
                () -> CompletableFuture.completedFuture(null)
                        .thenComposeAsync( v -> deleteGarbage(true, config.getGarbageCollectionConcurrency()), executor)
                        .handleAsync((v, ex) -> {
                            if (null != ex) {
                                log.error("{} Error during run.", chunkedSegmentStorage.getLogPrefix(), ex);
                            }
                            return null;
                        }, executor),
                executor);
        return loopFuture;
    }

    private boolean canRun() {
        return isRunning() && getStopException() == null && !closed.get();
    }

    /**
     * Sets whether background cleanup is suspended or not.
     *
     * @param value Boolean indicating whether to suspend background processing or not.
     */
    void setSuspended(boolean value) {
        suspended.set(value);
    }

    /**
     * collect the garbage chunks.
     *
     * @param chunksToDelete List of chunks to delete.
     */
    void addToGarbage(Collection<String> chunksToDelete) {
        val currentTime = System.currentTimeMillis();

        if (queueSize.get() < config.getGarbageCollectionMaxQueueSize()) {
            chunksToDelete.forEach(chunkToDelete -> garbageChunks.add(new GarbageChunkInfo(chunkToDelete, currentTime)));
            queueSize.incrementAndGet();
        } else {
            for (val chunkToDelete : chunksToDelete) {
                log.warn("{} deleteGarbage - Queue full. Could not delete garbage. chunk {}.", chunkedSegmentStorage.getLogPrefix(), chunkToDelete);
            }
        }
    }

    /**
     * Delete the garbage chunks.
     *
     * This method retrieves a few eligible chunks for deletion at a time.
     * The chunk is deleted only if the metadata for it does not exist or is marked inactive.
     * If there are any errors then failed chunk is enqueued back.
     * If suspended or there are no items then it "sleeps" for time specified by configuration.
     *
     * @param isBackground True if the caller is backgound task else False if called explicitly.
     * @param maxItems Maximum number of items to delete at a time.
     * @return CompletableFuture which is completed when garbage is deleted.
     */
    CompletableFuture<Void> deleteGarbage(boolean isBackground, int maxItems) {
        // Sleep if suspended.
        if (suspended.get() && isBackground) {
            log.info("{} deleteGarbage - suspended - sleeping for {}.", chunkedSegmentStorage.getLogPrefix(), config.getGarbageCollectionDelay());
            return Futures.delayedFuture(config.getGarbageCollectionSleep(), executor);
        }

        // Find chunks to delete.
        val currentTime = System.currentTimeMillis();
        val chunksToDelete = new ArrayList<GarbageChunkInfo>();
        int count = 0;
        val iterator = garbageChunks.iterator();
        while (iterator.hasNext()) {
            GarbageChunkInfo info = iterator.next();
            if (info == null || count >= maxItems) {
                break;
            }
            if (canDelete(info.getDeletedTime(), currentTime)) {
                queueSize.decrementAndGet();
                chunksToDelete.add(info);
                iterator.remove();
                count++;
            }
        }

        // Sleep if no chunks to delete.
        if (count == 0) {
            log.debug("{} deleteGarbage - no work - sleeping for {}.", chunkedSegmentStorage.getLogPrefix(), config.getGarbageCollectionDelay());
            return Futures.delayedFuture(config.getGarbageCollectionSleep(), executor);
        }

        // For each chunk delete if the chunk is not present at all in the metadata or is present but marked as inactive.
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (val infoToDelete : chunksToDelete) {
            val chunkToDelete = infoToDelete.name;
            val txn = chunkedSegmentStorage.getMetadataStore().beginTransaction(false, chunkToDelete);
            val future =
                    txn.get(infoToDelete.name)
                    .thenComposeAsync( metadata -> {
                            val chunkMetadata = (ChunkMetadata) metadata;
                            // Delete if the chunk is not present at all in the metadata or is present but marked as inactive.
                            boolean shouldDelete = (null == chunkMetadata) ? true : !chunkMetadata.isActive();
                            // Check whether the chunk is marked as inactive
                            if (shouldDelete) {
                                return chunkedSegmentStorage.getChunkStorage().openWrite(chunkToDelete)
                                        .thenComposeAsync(chunkedSegmentStorage.getChunkStorage()::delete, executor)
                                        .thenRunAsync(() -> {
                                            if (null != metadata) {
                                                txn.delete(chunkToDelete);
                                            }
                                            log.debug("{} deleteGarbage - deleted chunk={}.", chunkedSegmentStorage.getLogPrefix(), chunkToDelete);
                                        }, executor)
                                        .thenComposeAsync(v -> txn.commit(), executor)
                                        .handleAsync((v, e) -> {
                                            if (e != null) {
                                                val ex = Exceptions.unwrap(e);
                                                if (ex instanceof ChunkNotFoundException) {
                                                    // Ignore - nothing to do here.
                                                    log.debug("{} deleteGarbage - Could not delete garbage chunk {}.", chunkedSegmentStorage.getLogPrefix(), chunkToDelete);
                                                } else {
                                                    log.warn("{} deleteGarbage - Could not delete garbage chunk {}.", chunkedSegmentStorage.getLogPrefix(), chunkToDelete);
                                                    // Queue it back.
                                                    addToGarbage(Collections.singleton(chunkToDelete));
                                                }
                                            }
                                            return v;
                                        }, executor);
                            } else {
                                log.info("{} deleteGarbage - Chunk is not marked as garbage{}.", chunkedSegmentStorage.getLogPrefix(), chunkToDelete);
                                return CompletableFuture.completedFuture(null);
                            }
                        })
                    .whenCompleteAsync((v, ex) -> txn.close(), executor);
                    futures.add(future);
        }
        return Futures.allOf(futures);
    }

    boolean canDelete(long chunkDeletedTime, long currentTime) {
        return config.getGarbageCollectionDelay().toMillis() <= (currentTime - chunkDeletedTime);
    }

    @Override
    public void close() {
        Services.stopAsync(this, executor);
        if (!this.closed.get()) {
            loopFuture.cancel(true);
            closed.set(true);
            super.close();
        }
    }

    @Data
    @RequiredArgsConstructor
    static class GarbageChunkInfo {
        private final String name;
        private final long deletedTime;
    }
}