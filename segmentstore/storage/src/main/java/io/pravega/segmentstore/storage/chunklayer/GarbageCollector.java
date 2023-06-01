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
import io.pravega.common.ObjectBuilder;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.shared.NameUtils;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_GC_CHUNK_DELETED;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_GC_CHUNK_FAILED;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_GC_CHUNK_NEW;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_GC_CHUNK_QUEUED;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_GC_CHUNK_RETRY;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_GC_SEGMENT_FAILED;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_GC_SEGMENT_PROCESSED;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_GC_SEGMENT_QUEUED;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_GC_SEGMENT_RETRY;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_GC_TASK_PROCESSED;
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
public class GarbageCollector implements AutoCloseable, StatsReporter {

    private final ChunkStorage chunkStorage;

    private final ChunkMetadataStore metadataStore;

    private final ChunkedSegmentStorageConfig config;

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Keeps track of queue size.
     * Size is an expensive operation on DelayQueue.
     */
    @Getter
    private final AtomicInteger queueSize = new AtomicInteger();

    @Getter
    private final AtomicLong iterationId = new AtomicLong();

    private final Supplier<Long> currentTimeSupplier;

    private final Function<Duration, CompletableFuture<Void>> delaySupplier;

    private final ScheduledExecutorService storageExecutor;

    @Getter
    private AbstractTaskQueueManager<TaskInfo> taskQueue;

    private final String traceObjectId;

    @Getter
    private final String taskQueueName;

    @Getter
    private final String failedQueueName;

    /**
     * Instance of {@link MultiKeySequentialProcessor}.
     */
    private final MultiKeySequentialProcessor<String> taskScheduler;

    /**
     * Constructs a new instance.
     *
     * @param containerId     Container id of the owner container.
     * @param chunkStorage    ChunkStorage instance to use for writing all logs.
     * @param metadataStore   ChunkMetadataStore for owner container.
     * @param config          Configuration options for this ChunkedSegmentStorage instance.
     * @param executorService ScheduledExecutorService to use.
     */
    public GarbageCollector(int containerId, ChunkStorage chunkStorage,
                            ChunkMetadataStore metadataStore,
                            ChunkedSegmentStorageConfig config,
                            ScheduledExecutorService executorService) {
        this(containerId, chunkStorage, metadataStore, config, executorService,
                System::currentTimeMillis,
                duration -> Futures.delayedFuture(duration, executorService));
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
                            Function<Duration, CompletableFuture<Void>> delaySupplier) {
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.config = Preconditions.checkNotNull(config, "config");
        this.currentTimeSupplier = Preconditions.checkNotNull(currentTimeSupplier, "currentTimeSupplier");
        this.delaySupplier = Preconditions.checkNotNull(delaySupplier, "delaySupplier");
        this.storageExecutor = Preconditions.checkNotNull(storageExecutor, "storageExecutor");
        this.traceObjectId = String.format("GarbageCollector[%d]", containerId);
        this.taskQueueName = String.format("GC.queue.%d", containerId);
        this.failedQueueName = String.format("GC.failed.queue.%d", containerId);
        this.taskScheduler = new MultiKeySequentialProcessor<>(storageExecutor);
    }

    /**
     * Initializes this instance.
     *
     * @param taskQueue Task queue to use.
     */
    public CompletableFuture<Void> initialize(AbstractTaskQueueManager<TaskInfo> taskQueue) {
        this.taskQueue = Preconditions.checkNotNull(taskQueue, "taskQueue");
        return taskQueue.addQueue(this.taskQueueName, false)
                .thenComposeAsync(v -> taskQueue.addQueue(this.failedQueueName, true), storageExecutor);
    }

    /**
     * Adds given chunks to list of garbage chunks.
     *
     * @param chunksToDelete List of chunks to delete.
     */
    CompletableFuture<Void> addChunksToGarbage(long transactionId, Collection<String> chunksToDelete) {
        Preconditions.checkState(null != taskQueue, "taskQueue must not be null.");
        val futures = new ArrayList<CompletableFuture<Void>>();
        val startTime = currentTimeSupplier.get() + config.getGarbageCollectionDelay().toMillis();

        chunksToDelete.forEach(chunkToDelete -> futures.add(addChunkToGarbage(transactionId, chunkToDelete, startTime, 0)));
        return Futures.allOf(futures);
    }

    /**
     * Adds given chunk to list of garbage chunks.
     *
     * @param chunkToDelete Name of the chunk to delete.
     * @param startTime     Start time.
     * @param attempts      Number of attempts to delete this chunk so far.
     */
    CompletableFuture<Void> addChunkToGarbage(long transactionId, String chunkToDelete, long startTime, int attempts) {
        Preconditions.checkState(null != taskQueue, "taskQueue must not be null.");
        return taskQueue.addTask(taskQueueName, new TaskInfo(chunkToDelete, startTime, attempts, TaskInfo.DELETE_CHUNK, transactionId))
                .thenRunAsync(() -> {
                    queueSize.incrementAndGet();
                    SLTS_GC_CHUNK_QUEUED.inc();
                }, this.storageExecutor);
    }

    /**
     * Adds segment to the GC.
     *
     * @param transactionId   Transaction id.
     * @param segmentToDelete Name of segment to delete.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded.
     * If the operation failed, it will contain the cause of the failure.
     */
    CompletableFuture<Void> addSegmentToGarbage(long transactionId, String segmentToDelete) {
        Preconditions.checkState(null != taskQueue, "taskQueue must not be null.");
        val startTime = currentTimeSupplier.get() + config.getGarbageCollectionDelay().toMillis();
        return taskQueue.addTask(taskQueueName, new TaskInfo(segmentToDelete, startTime, 0, TaskInfo.DELETE_SEGMENT, transactionId))
                .thenRunAsync(() -> {
                    queueSize.incrementAndGet();
                    SLTS_GC_SEGMENT_QUEUED.inc();
                }, this.storageExecutor);
    }

    /**
     * Adds segment to the GC.
     *
     * @param taskInfo Task info
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded.
     * If the operation failed, it will contain the cause of the failure.
     */
    CompletableFuture<Void> addSegmentToGarbage(TaskInfo taskInfo) {
        Preconditions.checkState(null != taskQueue, "taskQueue must not be null.");
        return taskQueue.addTask(taskQueueName, taskInfo)
                .thenRunAsync(() -> {
                    queueSize.incrementAndGet();
                    SLTS_GC_SEGMENT_QUEUED.inc();
                }, this.storageExecutor);
    }

    /**
     * Adds new chunk to track
     *
     * @param transactionId TransactionId
     * @param chunktoTrack  Name of chunk to track.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded.
     * If the operation failed, it will contain the cause of the failure.
     */
    CompletableFuture<Void> trackNewChunk(long transactionId, String chunktoTrack) {
        Preconditions.checkState(null != taskQueue, "taskQueue must not be null.");
        val startTime = currentTimeSupplier.get() + config.getGarbageCollectionDelay().toMillis();
        // Simply add delete chunk task for newly tracked chunk and update metrics.
        return taskQueue.addTask(taskQueueName, new TaskInfo(chunktoTrack, startTime, 0, TaskInfo.DELETE_CHUNK, transactionId))
                .thenRunAsync(() -> {
                    queueSize.incrementAndGet();
                    SLTS_GC_CHUNK_NEW.inc();
                }, this.storageExecutor);
    }

    /**
     * Add the task to failed queue.
     */
    private CompletableFuture<Void> failTask(TaskInfo infoToRetire) {
        Preconditions.checkState(null != taskQueue, "taskQueue must not be null.");
        return taskQueue.addTask(failedQueueName, infoToRetire);
    }

    /**
     * Perform delete segment related tasks.
     */
    private CompletableFuture<Void> deleteSegment(TaskInfo taskInfo) {
        val streamSegmentName = taskInfo.getName();
        val txn = metadataStore.beginTransaction(true, streamSegmentName);
        return txn.get(streamSegmentName)
                .thenComposeAsync(storageMetadata -> {
                    val segmentMetadata = (SegmentMetadata) storageMetadata;
                    if (null == segmentMetadata) {
                        log.debug("{}: deleteGarbage - Segment metadata does not exist. segment={}.", traceObjectId, streamSegmentName);
                        return CompletableFuture.completedFuture(null);
                    } else if (segmentMetadata.isActive()) {
                        log.debug("{}: deleteGarbage - Segment is not marked as deleted. segment={}.", traceObjectId, streamSegmentName);
                        return CompletableFuture.completedFuture(null);
                    } else {
                        val chunksToDelete = Collections.synchronizedSet(new HashSet<String>());
                        val currentBatch = Collections.synchronizedSet(new HashSet<ChunkMetadata>());
                        val currentChunkName = new AtomicReference<String>(segmentMetadata.getFirstChunk());

                        return Futures.loop(
                                () -> null != currentChunkName.get(),
                                () -> txn.get(currentChunkName.get())
                                        .thenComposeAsync(metadata -> {
                                            val chunkMetadata = (ChunkMetadata) metadata;
                                            CompletableFuture<Void> retFuture = CompletableFuture.completedFuture(null);

                                            // Skip if metadata is possibly deleted in last attempt, we are done.
                                            if (null == chunkMetadata) {
                                                currentChunkName.set(null);
                                                return retFuture;
                                            }

                                            // Add to list of chunks to delete
                                            chunksToDelete.add(chunkMetadata.getName());

                                            // Add to batch and commit batch if required.
                                            currentBatch.add(chunkMetadata);
                                            if (chunkMetadata.isActive()) {
                                                if (currentBatch.size() > config.getGarbageCollectionTransactionBatchSize()) {
                                                    // Commit batch
                                                    retFuture = addTransactionForUpdateBatch(currentBatch, streamSegmentName);
                                                    // Clear batch
                                                    currentBatch.clear();
                                                }
                                            }
                                            // Move next
                                            currentChunkName.set(chunkMetadata.getNextChunk());
                                            return retFuture;
                                        }, storageExecutor),
                                storageExecutor)
                                .thenComposeAsync( v -> {
                                    if (currentBatch.size() > 0) {
                                        return addTransactionForUpdateBatch(currentBatch, streamSegmentName);
                                    }
                                    return CompletableFuture.completedFuture(null);
                                }, storageExecutor)
                                .thenComposeAsync(v -> this.addChunksToGarbage(txn.getVersion(), chunksToDelete), storageExecutor)
                                .thenComposeAsync(v -> deleteBlockIndexEntriesForSegment(streamSegmentName, segmentMetadata.getStartOffset(), segmentMetadata.getLength()), storageExecutor)
                                .thenComposeAsync(v -> {
                                    val innerTxn = metadataStore.beginTransaction(false, segmentMetadata.getName());
                                    innerTxn.delete(segmentMetadata.getName());
                                    return innerTxn.commit()
                                            .whenCompleteAsync((vv, ex) -> innerTxn.close(), storageExecutor);
                                }, storageExecutor)
                                .handleAsync((v, e) -> {
                                    txn.close();
                                    if (null != e) {
                                        log.error(String.format("%s deleteGarbage - Could not delete metadata for garbage segment=%s.",
                                                traceObjectId, streamSegmentName), e);
                                        return true;
                                    }
                                    return false;
                                }, storageExecutor)
                                .thenComposeAsync(failed -> {
                                    if (failed) {
                                        if (taskInfo.getAttempts() < config.getGarbageCollectionMaxAttempts()) {
                                            val attempts = taskInfo.attempts + 1;
                                            SLTS_GC_SEGMENT_RETRY.inc();
                                            return addSegmentToGarbage(taskInfo.toBuilder().attempts(attempts).build());
                                        } else {
                                            SLTS_GC_SEGMENT_FAILED.inc();
                                            log.info("{}: deleteGarbage - could not delete after max attempts segment={}.", traceObjectId, taskInfo.getName());
                                            return failTask(taskInfo);
                                        }
                                    } else {
                                        SLTS_GC_SEGMENT_PROCESSED.inc();
                                        return CompletableFuture.completedFuture(null);
                                    }
                                }, storageExecutor);
                    }
                }, storageExecutor);
    }

    private CompletableFuture<Void> addTransactionForUpdateBatch(Set<ChunkMetadata> batch, String name) {
        // create a sub transaction for a batch.
        val innerTxn = metadataStore.beginTransaction(false, name);
        for (val chunkMetadata : batch) {
            chunkMetadata.setActive(false);
            innerTxn.update(chunkMetadata);
        }
        return innerTxn.commit()
                .whenCompleteAsync((vv, ex) -> innerTxn.close(), storageExecutor);
    }

    /**
     * Delete block index entries for given chunk.
     */
    void deleteBlockIndexEntriesForChunk(MetadataTransaction txn, String segmentName, long startOffset, long endOffset) {
        val firstBlock = startOffset / config.getIndexBlockSize();
        for (long offset = firstBlock * config.getIndexBlockSize(); offset < endOffset; offset += config.getIndexBlockSize()) {
            txn.delete(NameUtils.getSegmentReadIndexBlockName(segmentName, offset));
        }
    }

    /**
     * Delete block index entries for given segment.
     */
    CompletableFuture<Void> deleteBlockIndexEntriesForSegment(String segmentName, long startOffset, long endOffset) {
        val firstBlock = startOffset / config.getIndexBlockSize();
        AtomicBoolean isDone = new AtomicBoolean(false);
        AtomicLong offset = new AtomicLong(firstBlock * config.getIndexBlockSize());

        return Futures.loop(
                () -> !isDone.get(),
                () -> {
                    val currentBatch = new HashSet<String>();
                    while (offset.get() < endOffset) {
                        val name = NameUtils.getSegmentReadIndexBlockName(segmentName, offset.get());
                        if (currentBatch.size() >= config.getGarbageCollectionTransactionBatchSize()) {
                            return addTransactionForDeleteBatch(currentBatch, segmentName);
                        }
                        currentBatch.add(name);
                        offset.addAndGet(config.getIndexBlockSize());
                    }
                    // We are done
                    isDone.set(true);
                    if (currentBatch.size() > 0) {
                        return addTransactionForDeleteBatch(currentBatch, segmentName);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                },
                storageExecutor);
    }

    private CompletableFuture<Void> addTransactionForDeleteBatch(Set<String> batch, String segmentName) {
        // create a sub transaction for a batch.
        val innerTxn = metadataStore.beginTransaction(false, segmentName);
        for (val entryName : batch) {
            innerTxn.delete(entryName);
        }
        return innerTxn.commit()
                .whenCompleteAsync((vv, ex) -> innerTxn.close(), storageExecutor);
    }

    /**
     * Process a batch of tasks.
     *
     * @param batch List of {@link TaskInfo} to process.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded.
     * If the operation failed, it will contain the cause of the failure.
     */
    public CompletableFuture<Void> processBatch(List<TaskInfo> batch) {
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (val infoToDelete : batch) {
            if (metadataStore.isTransactionActive(infoToDelete.transactionId)) {
                log.debug("{}: deleteGarbage - transaction is still active - re-queuing {}.", traceObjectId, infoToDelete.transactionId);
                taskQueue.addTask(taskQueueName, infoToDelete);
            } else {
                val f = executeSerialized(() -> processTask(infoToDelete), infoToDelete.name);
                val now = currentTimeSupplier.get();
                if (infoToDelete.scheduledTime > currentTimeSupplier.get()) {
                    futures.add(delaySupplier.apply(Duration.ofMillis(infoToDelete.scheduledTime - now))
                            .thenComposeAsync(v -> f, storageExecutor));
                } else {
                    futures.add(f);
                }
            }
        }
        return Futures.allOf(futures)
                .thenRunAsync(() -> {
                    queueSize.addAndGet(-batch.size());
                    SLTS_GC_TASK_PROCESSED.add(batch.size());
                }, storageExecutor);
    }

    /**
     * Executes the given Callable asynchronously and returns a CompletableFuture that will be completed with the result.
     * The operations are serialized on the segmentNames provided.
     *
     * @param operation The Callable to execute.
     * @param <R>       Return type of the operation.
     * @param keyNames  The names of the keys involved in this operation (for sequencing purposes).
     * @return A CompletableFuture that, when completed, will contain the result of the operation.
     * If the operation failed, it will contain the cause of the failure.
     */
    private <R> CompletableFuture<R> executeSerialized(Callable<CompletableFuture<R>> operation, String... keyNames) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.taskScheduler.add(Arrays.asList(keyNames), () -> executeExclusive(operation, keyNames));
    }

    /**
     * Executes the given Callable asynchronously and exclusively.
     * It returns a CompletableFuture that will be completed with the result.
     * The operations are not allowed to be concurrent.
     *
     * @param operation The Callable to execute.
     * @param <R>       Return type of the operation.
     * @param keyNames  The names of the keys involved in this operation (for sequencing purposes).
     * @return A CompletableFuture that, when completed, will contain the result of the operation.
     * If the operation failed, it will contain the cause of the failure.
     */
    private <R> CompletableFuture<R> executeExclusive(Callable<CompletableFuture<R>> operation, String... keyNames) {
        return CompletableFuture.completedFuture(null).thenComposeAsync(v -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            try {
                return operation.call();
            } catch (Exception e) {
                throw new CompletionException(Exceptions.unwrap(e));
            }
        }, this.storageExecutor);
    }

    private CompletableFuture<Void> processTask(TaskInfo infoToDelete) {
        if (infoToDelete.taskType == TaskInfo.DELETE_CHUNK) {
            return deleteChunk(infoToDelete);
        }
        if (infoToDelete.taskType == TaskInfo.DELETE_SEGMENT) {
            return deleteSegment(infoToDelete);
        }
        if (infoToDelete.taskType == TaskInfo.DELETE_JOURNAL) {
            return deleteChunk(infoToDelete);
        }
        log.info("{}: processTask - Ignoring unknown type of task {}.", traceObjectId, infoToDelete);
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> deleteChunk(TaskInfo infoToDelete) {
        val chunkToDelete = infoToDelete.name;
        val failed = new AtomicReference<Throwable>();
        val txn = metadataStore.beginTransaction(false, chunkToDelete);
        return txn.get(infoToDelete.name)
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
                                            failed.set(e);
                                        }
                                    } else {
                                        SLTS_GC_CHUNK_DELETED.inc();
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
                                        failed.set(e);
                                    }
                                    return v;
                                }, storageExecutor);
                    } else {
                        log.debug("{}: deleteGarbage - Chunk is not marked as garbage chunk={}.", traceObjectId, chunkToDelete);
                        return CompletableFuture.completedFuture(null);
                    }
                }, storageExecutor)
                .thenComposeAsync(v -> {
                    if (failed.get() != null) {
                        if (infoToDelete.getAttempts() < config.getGarbageCollectionMaxAttempts()) {
                            log.debug("{}: deleteGarbage - adding back chunk={}.", traceObjectId, chunkToDelete);
                            SLTS_GC_CHUNK_RETRY.inc();
                            return addChunkToGarbage(txn.getVersion(), chunkToDelete,
                                    infoToDelete.getScheduledTime() + config.getGarbageCollectionDelay().toMillis(),
                                    infoToDelete.getAttempts() + 1);
                        } else {
                            SLTS_GC_CHUNK_FAILED.inc();
                            log.info("{}: deleteGarbage - could not delete after max attempts chunk={}.", traceObjectId, chunkToDelete);
                            return failTask(infoToDelete);

                        }
                    }
                    return CompletableFuture.completedFuture(null);
                }, storageExecutor)
                .whenCompleteAsync((v, ex) -> {
                    if (ex != null) {
                        log.error(String.format("%s deleteGarbage - Could not find garbage chunk=%s.",
                                traceObjectId, chunkToDelete), ex);
                    }
                    txn.close();
                }, storageExecutor);
    }

    @Override
    public void close() throws Exception {
        if (!this.closed.get()) {
            if (null != taskQueue) {
                this.taskQueue.close();
            }
            closed.set(true);
        }
    }

    @Override
    public void report() {
        ChunkStorageMetrics.DYNAMIC_LOGGER.reportGaugeValue(SLTS_GC_QUEUE_SIZE, queueSize.get());
    }

    /**
     * Represents a Task info.
     */
    public static abstract class AbstractTaskInfo {
        public static final int DELETE_CHUNK = 1;
        public static final int DELETE_SEGMENT = 2;
        public static final int DELETE_JOURNAL = 3;

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class AbstractTaskInfoSerializer extends VersionedSerializer.MultiType<AbstractTaskInfo> {
            /**
             * Declare all supported serializers of subtypes.
             *
             * @param builder A MultiType.Builder that can be used to declare serializers.
             */
            @Override
            protected void declareSerializers(Builder builder) {
                // Unused values (Do not repurpose!):
                // - 0: Unsupported Serializer.
                builder.serializer(TaskInfo.class, 1, new TaskInfo.Serializer());
            }
        }
    }

    /**
     * Represents background task.
     */
    @Data
    @RequiredArgsConstructor
    @Builder(toBuilder = true)
    @EqualsAndHashCode(callSuper = true)
    public static class TaskInfo extends AbstractTaskInfo {
        @NonNull
        private final String name;
        private final long scheduledTime;
        private final int attempts;
        private final int taskType;
        private final long transactionId;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class TaskInfoBuilder implements ObjectBuilder<TaskInfo> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class Serializer extends VersionedSerializer.WithBuilder<TaskInfo, TaskInfo.TaskInfoBuilder> {
            @Override
            protected TaskInfo.TaskInfoBuilder newBuilder() {
                return TaskInfo.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(TaskInfo object, RevisionDataOutput output) throws IOException {
                output.writeUTF(object.name);
                output.writeCompactLong(object.scheduledTime);
                output.writeCompactInt(object.attempts);
                output.writeCompactInt(object.taskType);
                output.writeLong(object.transactionId);
            }

            private void read00(RevisionDataInput input, TaskInfo.TaskInfoBuilder b) throws IOException {
                b.name(input.readUTF());
                b.scheduledTime(input.readCompactLong());
                b.attempts(input.readCompactInt());
                b.taskType(input.readCompactInt());
                b.transactionId(input.readLong());
            }
        }
    }
}
