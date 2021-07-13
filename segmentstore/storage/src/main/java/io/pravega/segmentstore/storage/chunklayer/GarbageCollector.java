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
import com.google.common.primitives.Ints;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.AbstractThreadPoolService;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.pravega.shared.MetricsNames.SLTS_GC_QUEUE_SIZE;

/**
 * Implements simple garbage collector for cleaning up the deleted chunks.
 * The garbage collector maintains a in memory queue of chunks to delete which is drained by a background task.
 * This queue is populated by following
 * <ol>
 * <li>Various ChunkedSegmentStorage operations requesting deletes.</li>
 * <li>Background task that scans all records to find undeleted chunks inside metadata (not yet implemented).</li>
 * <li>Background task that scans all LTS to find unaccounted chunks that are deemed garbage (not yet implemented)</li>
 * </ol>
 *
 * The background task throttles itself in two ways.
 * <ol>
 * <li>It limits number of concurrent deletes at a time, so that it doesn't interfere with foreground Storage calls. </li>
 * <li>It limits the number of items in the queue.</li>
 * </ol>
 */
@Slf4j
public class GarbageCollector extends AbstractThreadPoolService implements AutoCloseable, StatsReporter {
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    /**
     * Set of garbage chunks.
     */
    @Getter
    private final DelayQueue<GarbageChunkInfo> garbageChunks = new DelayQueue<>();

    private final ChunkStorage chunkStorage;

    private final ChunkMetadataStore metadataStore;

    private final ChunkedSegmentStorageConfig config;

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicBoolean suspended = new AtomicBoolean();

    /**
     * Keeps track of queue size.
     * Size is an expensive operation on DelayQueue.
     */
    @Getter
    private final AtomicInteger queueSize = new AtomicInteger();

    @Getter
    private final AtomicLong iterationId = new AtomicLong();

    private CompletableFuture<Void> loopFuture;

    private final Supplier<Long> currentTimeSupplier;

    private final Supplier<CompletableFuture<Void>> delaySupplier;

    private final ScheduledExecutorService storageExecutor;

    /**
     * Constructs a new instance.
     *
     * @param containerId         Container id of the owner container.
     * @param chunkStorage        ChunkStorage instance to use for writing all logs.
     * @param metadataStore       ChunkMetadataStore for owner container.
     * @param config              Configuration options for this ChunkedSegmentStorage instance.
     * @param executorService     ScheduledExecutorService to use.
     */
    public GarbageCollector(int containerId, ChunkStorage chunkStorage,
                            ChunkMetadataStore metadataStore,
                            ChunkedSegmentStorageConfig config,
                            ScheduledExecutorService executorService) {
        this(containerId, chunkStorage, metadataStore, config, executorService,
                System::currentTimeMillis,
                () -> Futures.delayedFuture(config.getGarbageCollectionSleep(), executorService));
    }

    /**
     * Constructs a new instance.
     *
     * @param containerId         Container id of the owner container.
     * @param chunkStorage        ChunkStorage instance to use for writing all logs.
     * @param metadataStore       ChunkMetadataStore for owner container.
     * @param config              Configuration options for this ChunkedSegmentStorage instance.
     * @param storageExecutor     ScheduledExecutorService to use for storage operations.
     * @param currentTimeSupplier Function that supplies current time.
     * @param delaySupplier       Function that supplies delay future.
     */
    public GarbageCollector(int containerId, ChunkStorage chunkStorage,
                            ChunkMetadataStore metadataStore,
                            ChunkedSegmentStorageConfig config,
                            ScheduledExecutorService storageExecutor,
                            Supplier<Long> currentTimeSupplier,
                            Supplier<CompletableFuture<Void>> delaySupplier) {
        super(String.format("GarbageCollector[%d]", containerId), ExecutorServiceHelpers.newScheduledThreadPool(1, "storage-gc"));
        try {
            this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
            this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
            this.config = Preconditions.checkNotNull(config, "config");
            this.currentTimeSupplier = Preconditions.checkNotNull(currentTimeSupplier, "currentTimeSupplier");
            this.delaySupplier = Preconditions.checkNotNull(delaySupplier, "delaySupplier");
            this.storageExecutor = Preconditions.checkNotNull(storageExecutor, "storageExecutor");
        } catch (Exception ex) {
            this.executor.shutdownNow();
            throw ex;
        }
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
        return SHUTDOWN_TIMEOUT;
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
                () -> delaySupplier.get()
                        .thenComposeAsync(v -> deleteGarbage(true, config.getGarbageCollectionMaxConcurrency()), executor)
                        .handleAsync((v, ex) -> {
                            if (null != ex) {
                                log.error("{}: Error during doRun.", traceObjectId, ex);
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
     * Adds given chunks to list of garbage chunks.
     *
     * @param chunksToDelete List of chunks to delete.
     */
    void addToGarbage(Collection<String> chunksToDelete) {
        val currentTime = currentTimeSupplier.get();

        chunksToDelete.forEach(chunkToDelete -> addToGarbage(chunkToDelete, currentTime + config.getGarbageCollectionDelay().toMillis(), 0));

        if (queueSize.get() >= config.getGarbageCollectionMaxQueueSize()) {
            log.warn("{}: deleteGarbage - Queue full. Could not delete garbage. Chunks skipped", traceObjectId);
        }
    }

    /**
     * Adds given chunk to list of garbage chunks.
     *
     * @param chunkToDelete Name of the chunk to delete.
     * @param startTime Start time.
     * @param attempts Number of attempts to delete this chunk so far.
     */
    void addToGarbage(String chunkToDelete, long startTime, int attempts) {
        if (queueSize.get() < config.getGarbageCollectionMaxQueueSize()) {
            garbageChunks.add(new GarbageChunkInfo(chunkToDelete, startTime, attempts));
            queueSize.incrementAndGet();
        } else {
            log.debug("{}: deleteGarbage - Queue full. Could not delete garbage. chunk {}.", traceObjectId, chunkToDelete);
        }
    }

    /**
     * Delete the garbage chunks.
     *
     * This method retrieves a few eligible chunks for deletion at a time.
     * The chunk is deleted only if the metadata for it does not exist or is marked inactive.
     * If there are any errors then failed chunk is enqueued back up to a max number of attempts.
     * If suspended or there are no items then it "sleeps" for time specified by configuration.
     *
     * @param isBackground True if the caller is background task else False if called explicitly.
     * @param maxItems     Maximum number of items to delete at a time.
     * @return CompletableFuture which is completed when garbage is deleted.
     */
    CompletableFuture<Boolean> deleteGarbage(boolean isBackground, int maxItems) {
        log.debug("{}: Iteration {} started.", traceObjectId, iterationId.get());
        // Sleep if suspended.
        if (suspended.get() && isBackground) {
            log.info("{}: deleteGarbage - suspended - sleeping for {}.", traceObjectId, config.getGarbageCollectionDelay());
            return CompletableFuture.completedFuture(false);
        }

        // Find chunks to delete.
        val chunksToDelete = new ArrayList<GarbageChunkInfo>();
        int count = 0;

        // Wait until you have at least one item or timeout expires.
        GarbageChunkInfo info = Exceptions.handleInterruptedCall(() -> garbageChunks.poll(config.getGarbageCollectionDelay().toMillis(), TimeUnit.MILLISECONDS));
        log.trace("{}: deleteGarbage - retrieved {}", traceObjectId, info);
        while (null != info ) {
            queueSize.decrementAndGet();
            chunksToDelete.add(info);

            count++;
            if (count >= maxItems) {
                break;
            }
            // Do not block
            info = garbageChunks.poll();
            log.trace("{}: deleteGarbage - retrieved {}", traceObjectId, info);
        }

        // Sleep if no chunks to delete.
        if (count == 0) {
            log.debug("{}: deleteGarbage - no work - sleeping for {}.", traceObjectId, config.getGarbageCollectionDelay());
            return CompletableFuture.completedFuture(false);
        }

        // For each chunk delete if the chunk is not present at all in the metadata or is present but marked as inactive.
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (val infoToDelete : chunksToDelete) {
            val chunkToDelete = infoToDelete.name;
            val failed = new AtomicBoolean();
            val txn = metadataStore.beginTransaction(false, chunkToDelete);
            val future =
                    txn.get(infoToDelete.name)
                            .thenComposeAsync(metadata -> {
                                val chunkMetadata = (ChunkMetadata) metadata;
                                // Delete if the chunk is not present at all in the metadata or is present but marked as inactive.
                                val shouldDeleteChunk = null == chunkMetadata || !chunkMetadata.isActive();
                                val shouldDeleteMetadata = new AtomicBoolean(null != metadata && !chunkMetadata.isActive());

                                // Delete chunk from storage.
                                if (shouldDeleteChunk) {
                                    return chunkStorage.delete(ChunkHandle.writeHandle(chunkToDelete))
                                            .handleAsync((v, e) -> {
                                                if (e != null) {
                                                    val ex = Exceptions.unwrap(e);
                                                    if (ex instanceof ChunkNotFoundException) {
                                                        // Ignore - nothing to do here.
                                                        log.debug("{}: deleteGarbage - Could not delete garbage chunk={}.", traceObjectId, chunkToDelete);
                                                    } else {
                                                        log.warn("{}: deleteGarbage - Could not delete garbage chunk={}.", traceObjectId, chunkToDelete);
                                                        shouldDeleteMetadata.set(false);
                                                        failed.set(true);
                                                    }
                                                } else {
                                                    log.debug("{}: deleteGarbage - deleted chunk={}.", traceObjectId, chunkToDelete);
                                                }
                                                return v;
                                            }, storageExecutor)
                                            .thenRunAsync(() -> {
                                                if (shouldDeleteMetadata.get()) {
                                                    txn.delete(chunkToDelete);
                                                    log.debug("{}: deleteGarbage - deleted metadata for chunk={}.", traceObjectId, chunkToDelete);
                                                }
                                            }, storageExecutor)
                                            .thenComposeAsync(v -> txn.commit(), storageExecutor)
                                            .handleAsync((v, e) -> {
                                                if (e != null) {
                                                    log.error(String.format("%s deleteGarbage - Could not delete metadata for garbage chunk=%s.",
                                                            traceObjectId, chunkToDelete), e);
                                                    failed.set(true);
                                                }
                                                return v;
                                            }, storageExecutor);
                                } else {
                                    log.info("{}: deleteGarbage - Chunk is not marked as garbage chunk={}.", traceObjectId, chunkToDelete);
                                    return CompletableFuture.completedFuture(null);
                                }
                            }, storageExecutor)
                            .whenCompleteAsync((v, ex) -> {
                                // Queue it back.
                                if (failed.get()) {
                                    if (infoToDelete.getAttempts() < config.getGarbageCollectionMaxAttempts()) {
                                        log.debug("{}: deleteGarbage - adding back chunk={}.", traceObjectId, chunkToDelete);
                                        addToGarbage(chunkToDelete,
                                                infoToDelete.getScheduledDeleteTime() + config.getGarbageCollectionDelay().toMillis(),
                                                infoToDelete.getAttempts() + 1);
                                    } else {
                                        log.info("{}: deleteGarbage - could not delete after max attempts chunk={}.", traceObjectId, chunkToDelete);
                                    }
                                }
                                if (ex != null) {
                                    log.error(String.format("%s deleteGarbage - Could not find garbage chunk=%s.",
                                            traceObjectId, chunkToDelete), ex);
                                }
                                txn.close();
                            }, executor);
            futures.add(future);
        }
        return Futures.allOf(futures)
                .thenApplyAsync( v -> {
                    log.debug("{}: Iteration {} ended.", traceObjectId, iterationId.getAndIncrement());
                    return true;
                }, executor);
    }

    @Override
    public void close() {
        Services.stopAsync(this, executor);
        if (!this.closed.get()) {
            if (null != loopFuture) {
                loopFuture.cancel(true);
            }
            closed.set(true);
            executor.shutdownNow();
            super.close();
        }
    }

    @Override
    public void report() {
        ChunkStorageMetrics.DYNAMIC_LOGGER.reportGaugeValue(SLTS_GC_QUEUE_SIZE, queueSize.get());
    }

    @RequiredArgsConstructor
    @Data
    class GarbageChunkInfo implements Delayed {
        @Getter
        private final String name;
        private final long scheduledDeleteTime;
        private final int attempts;

        @Override
        public long getDelay(TimeUnit timeUnit) {
            return timeUnit.convert(scheduledDeleteTime - currentTimeSupplier.get(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed delayed) {
            return Ints.saturatedCast(scheduledDeleteTime - ((GarbageChunkInfo) delayed).scheduledDeleteTime);
        }
    }
}
