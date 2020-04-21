/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.logs.operations.CompletableOperation;
import io.pravega.segmentstore.storage.cache.CacheState;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static io.pravega.shared.MetricsTags.containerTag;
import static io.pravega.shared.MetricsTags.throttlerTag;

/**
 * General Metrics for the SegmentStore.
 */
public final class SegmentStoreMetrics {
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("segmentstore");

    /**
     * Global (not container-specific) end-to-end latency of an operation from when it enters the OperationProcessor
     * until it is completed.
     */
    private static final OpStatsLogger GLOBAL_OPERATION_LATENCY = STATS_LOGGER.createStats(MetricsNames.OPERATION_LATENCY);

    //region CacheManager

    /**
     * CacheManager metrics.
     */
    public final static class CacheManager {
        public void report(CacheState snapshot, int generationSpread) {
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.CACHE_STORED_SIZE_BYTES, snapshot.getStoredBytes());
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.CACHE_USED_SIZE_BYTES, snapshot.getUsedBytes());
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.CACHE_ALLOC_SIZE_BYTES, snapshot.getAllocatedBytes());
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.CACHE_GENERATION_SPREAD, generationSpread);
        }
    }

    //endregion

    //region ThreadPool

    /**
     * SegmentStore ThreadPool metrics.
     */
    public final static class ThreadPool implements AutoCloseable {
        private final OpStatsLogger queueSize;
        private final OpStatsLogger activeThreads;
        private final ScheduledExecutorService executor;
        private final ScheduledFuture<?> reporter;

        public ThreadPool(ScheduledExecutorService executor) {
            this.executor = Preconditions.checkNotNull(executor, "executor");
            this.queueSize = STATS_LOGGER.createStats(MetricsNames.THREAD_POOL_QUEUE_SIZE);
            this.activeThreads = STATS_LOGGER.createStats(MetricsNames.THREAD_POOL_ACTIVE_THREADS);
            this.reporter = executor.scheduleWithFixedDelay(this::report, 1000, 1000, TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() {
            this.reporter.cancel(true);
            this.queueSize.close();
            this.activeThreads.close();
        }

        private void report() {
            ExecutorServiceHelpers.Snapshot s = ExecutorServiceHelpers.getSnapshot(this.executor);
            if (s != null) {
                this.queueSize.reportSuccessValue(s.getQueueSize());
                this.activeThreads.reportSuccessValue(s.getActiveThreadCount());
            }
        }
    }

    //endregion

    //region OperationProcessor

    /**
     * OperationProcessor metrics.
     */
    public final static class OperationProcessor implements AutoCloseable {
        /**
         * Number of items in the Operation Queue.
         */
        private final OpStatsLogger operationQueueSize;

        /**
         * Number of items out of the Operation Queue, sent to Tier1, but not yet acknowledged.
         */
        private final OpStatsLogger operationsInFlight;

        /**
         * Amount of time an operation spends in the queue, before being picked up.
         */
        private final OpStatsLogger operationQueueWaitTime;

        /**
         * Amount of time spent committing an operation after being written to Tier1 (this includes in-memory structures
         * and Cache).
         */
        private final OpStatsLogger operationCommitLatency;

        /**
         * Container-specific, end-to-end latency of an operation from when it enters the OperationProcessor until it is completed.
         */
        private final OpStatsLogger operationLatency;

        /**
         * Number of ContainerMetadataUpdateTransactions committed at once.
         */
        private final OpStatsLogger memoryCommitCount;

        /**
         * Amount of time elapsed to commit operations to memory, including commit to Metadata, adding to InMemoryLog
         * and ReadIndex.
         */
        private final OpStatsLogger memoryCommitLatency;

        /**
         * Amount of time spent inside processOperations(Queue)
         */
        private final OpStatsLogger processOperationsLatency;
        private final OpStatsLogger processOperationsBatchSize;
        private final int containerId;
        private final String[] containerTag;
        private Set<String> throttlers = Collections.synchronizedSet(new HashSet<>());

        public OperationProcessor(int containerId) {
            this.containerId = containerId;
            this.containerTag = containerTag(containerId);
            this.operationQueueSize = STATS_LOGGER.createStats(MetricsNames.OPERATION_QUEUE_SIZE, this.containerTag);
            this.operationsInFlight = STATS_LOGGER.createStats(MetricsNames.OPERATION_PROCESSOR_IN_FLIGHT, this.containerTag);
            this.operationQueueWaitTime = STATS_LOGGER.createStats(MetricsNames.OPERATION_QUEUE_WAIT_TIME, this.containerTag);
            this.operationCommitLatency = STATS_LOGGER.createStats(MetricsNames.OPERATION_COMMIT_LATENCY, this.containerTag);
            this.operationLatency = STATS_LOGGER.createStats(MetricsNames.OPERATION_LATENCY, this.containerTag);
            this.memoryCommitLatency = STATS_LOGGER.createStats(MetricsNames.OPERATION_COMMIT_MEMORY_LATENCY, this.containerTag);
            this.memoryCommitCount = STATS_LOGGER.createStats(MetricsNames.OPERATION_COMMIT_MEMORY_COUNT, this.containerTag);
            this.processOperationsLatency = STATS_LOGGER.createStats(MetricsNames.PROCESS_OPERATIONS_LATENCY, this.containerTag);
            this.processOperationsBatchSize = STATS_LOGGER.createStats(MetricsNames.PROCESS_OPERATIONS_BATCH_SIZE, this.containerTag);
        }

        @Override
        public void close() {
            this.operationQueueSize.close();
            this.operationsInFlight.close();
            this.operationQueueWaitTime.close();
            this.operationCommitLatency.close();
            this.operationLatency.close();
            this.memoryCommitLatency.close();
            this.memoryCommitCount.close();
            this.processOperationsLatency.close();
            this.processOperationsBatchSize.close();
            for (String throttler : throttlers) {
                DYNAMIC_LOGGER.freezeGaugeValue(MetricsNames.OPERATION_PROCESSOR_DELAY_MILLIS, throttlerTag(containerId, throttler));
            }
        }

        public void currentState(int queueSize, int inFlightCount) {
            this.operationQueueSize.reportSuccessValue(queueSize);
            this.operationsInFlight.reportSuccessValue(inFlightCount);
        }

        public void processingDelay(int millis, String throttlerName) {
            throttlers.add(throttlerName);
            DYNAMIC_LOGGER.reportGaugeValue(
                    MetricsNames.OPERATION_PROCESSOR_DELAY_MILLIS,
                    millis,
                    throttlerTag(this.containerId, throttlerName)
            );
        }

        public void operationQueueWaitTime(long queueWaitTimeMillis) {
            this.operationQueueWaitTime.reportSuccessValue(queueWaitTimeMillis);
        }

        public void memoryCommit(int commitCount, Duration elapsed) {
            this.memoryCommitCount.reportSuccessValue(commitCount);
            this.memoryCommitLatency.reportSuccessEvent(elapsed);
        }

        public void operationLogTruncate(int count) {
            DYNAMIC_LOGGER.incCounterValue(MetricsNames.OPERATION_LOG_SIZE, -count, this.containerTag);
        }

        public void operationLogInit() {
            DYNAMIC_LOGGER.updateCounterValue(MetricsNames.OPERATION_LOG_SIZE, 0, this.containerTag);
        }

        public void processOperations(int batchSize, long millis) {
            this.processOperationsBatchSize.reportSuccessValue(batchSize);
            this.processOperationsLatency.reportSuccessValue(millis);
        }

        public void operationsCompleted(int operationCount, Duration commitElapsed) {
            DYNAMIC_LOGGER.incCounterValue(MetricsNames.OPERATION_LOG_SIZE, operationCount, this.containerTag);
            this.operationCommitLatency.reportSuccessEvent(commitElapsed);
        }

        public void operationsCompleted(Collection<List<CompletableOperation>> operations, Duration commitElapsed) {
            operationsCompleted(operations.size(), commitElapsed);
            operations.stream().flatMap(List::stream).forEach(o -> {
                long millis = o.getTimer().getElapsedMillis();
                this.operationLatency.reportSuccessValue(millis);
                GLOBAL_OPERATION_LATENCY.reportSuccessValue(millis);
            });
        }

        public void operationsFailed(Collection<CompletableOperation> operations) {
            operations.forEach(o -> {
                long millis = o.getTimer().getElapsedMillis();
                this.operationLatency.reportFailValue(millis);
                GLOBAL_OPERATION_LATENCY.reportFailValue(millis);
            });
        }
    }

    //endregion

    //region Metadata

    /**
     * ContainerMetadata metrics.
     */
    public final static class Metadata {
        private final String[] containerTag;

        public Metadata(int containerId) {
            this.containerTag = containerTag(containerId);
        }

        public void segmentCount(int count) {
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.ACTIVE_SEGMENT_COUNT, count, this.containerTag);
        }
    }

    //endregion

    //region Container

    /**
     * StreamSegmentContainer Metrics.
     */
    public final static class Container implements AutoCloseable {
        private final String[] containerTag;

        public Container(int containerId) {
            this.containerTag = containerTag(containerId);
        }

        public void createSegment() {
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_CREATE_SEGMENT_COUNT, 1, containerTag);
        }

        public void deleteSegment() {
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_DELETE_SEGMENT_COUNT, 1, containerTag);
        }

        public void append() {
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_APPEND_COUNT, 1, containerTag);
        }

        public void appendWithOffset() {
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_APPEND_OFFSET_COUNT, 1, containerTag);
        }

        public void updateAttributes() {
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_UPDATE_ATTRIBUTES_COUNT, 1, containerTag);
        }

        public void getAttributes() {
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_GET_ATTRIBUTES_COUNT, 1, containerTag);
        }

        public void read() {
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_READ_COUNT, 1, containerTag);
        }

        public void getInfo() {
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_GET_INFO_COUNT, 1, containerTag);
        }

        public void mergeSegment() {
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_MERGE_SEGMENT_COUNT, 1, containerTag);
        }

        public void seal() {
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_SEAL_COUNT, 1, containerTag);
        }

        public void truncate() {
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_TRUNCATE_COUNT, 1, containerTag);
        }

        @Override
        public void close() {
            DYNAMIC_LOGGER.freezeMeter(MetricsNames.CONTAINER_CREATE_SEGMENT_COUNT, containerTag);
            DYNAMIC_LOGGER.freezeMeter(MetricsNames.CONTAINER_DELETE_SEGMENT_COUNT, containerTag);
            DYNAMIC_LOGGER.freezeMeter(MetricsNames.CONTAINER_MERGE_SEGMENT_COUNT, containerTag);
            DYNAMIC_LOGGER.freezeMeter(MetricsNames.CONTAINER_APPEND_COUNT, containerTag);
            DYNAMIC_LOGGER.freezeMeter(MetricsNames.CONTAINER_APPEND_OFFSET_COUNT, containerTag);
            DYNAMIC_LOGGER.freezeMeter(MetricsNames.CONTAINER_UPDATE_ATTRIBUTES_COUNT, containerTag);
            DYNAMIC_LOGGER.freezeMeter(MetricsNames.CONTAINER_GET_ATTRIBUTES_COUNT, containerTag);
            DYNAMIC_LOGGER.freezeMeter(MetricsNames.CONTAINER_READ_COUNT, containerTag);
            DYNAMIC_LOGGER.freezeMeter(MetricsNames.CONTAINER_GET_INFO_COUNT, containerTag);
            DYNAMIC_LOGGER.freezeMeter(MetricsNames.CONTAINER_SEAL_COUNT, containerTag);
            DYNAMIC_LOGGER.freezeMeter(MetricsNames.CONTAINER_TRUNCATE_COUNT, containerTag);
        }
    }

    //endregion

    //region StorageWriter

    /**
     * StorageWriter metrics.
     */
    public final static class StorageWriter implements AutoCloseable {
        /**
         * Time elapsed for flushing all processors.
         */
        private final OpStatsLogger flushElapsed;
        /**
         * Time elapsed for an iteration.
         */
        private final OpStatsLogger iterationElapsed;
        /**
         * Number of bytes flushed to Storage.
         */
        private final Counter flushedBytes;
        /**
         * Number of bytes merged in Storage.
         */
        private final Counter mergedBytes;
        /**
         * Number of attributes flushed to Storage.
         */
        private final Counter flushedAttributes;
        /**
         * Number of operations read from DurableLog.
         */
        private final Counter readCount;

        public StorageWriter(int containerId) {
            String[] containerTag = containerTag(containerId);
            this.flushElapsed = STATS_LOGGER.createStats(MetricsNames.STORAGE_WRITER_FLUSH_ELAPSED, containerTag);
            this.iterationElapsed = STATS_LOGGER.createStats(MetricsNames.STORAGE_WRITER_ITERATION_ELAPSED, containerTag);
            this.readCount = STATS_LOGGER.createCounter(MetricsNames.STORAGE_WRITER_READ_COUNT, containerTag);
            this.flushedBytes = STATS_LOGGER.createCounter(MetricsNames.STORAGE_WRITER_FLUSHED_BYTES, containerTag);
            this.mergedBytes = STATS_LOGGER.createCounter(MetricsNames.STORAGE_WRITER_MERGED_BYTES, containerTag);
            this.flushedAttributes = STATS_LOGGER.createCounter(MetricsNames.STORAGE_WRITER_FLUSHED_ATTRIBUTES, containerTag);
        }

        @Override
        public void close() {
            this.readCount.close();
            this.flushElapsed.close();
            this.iterationElapsed.close();
            this.flushedBytes.close();
            this.mergedBytes.close();
            this.flushedAttributes.close();
        }

        public void readComplete(int operationCount) {
            this.readCount.add(operationCount);
        }

        public void flushComplete(long flushedBytes, long mergedBytes, int flushedAttributes, Duration elapsed) {
            this.flushedBytes.add(flushedBytes);
            this.mergedBytes.add(mergedBytes);
            this.flushedAttributes.add(flushedAttributes);
            this.flushElapsed.reportSuccessEvent(elapsed);
        }

        public void iterationComplete(Duration elapsed) {
            this.iterationElapsed.reportSuccessEvent(elapsed);
        }
    }

    //endregion

    //region RecoveryProcessor

    /**
     * Reports the time taken for a container recovery.
     *
     * @param duration Time taken for a Segment Store instance to perform the recovery of containers.
     * @param containerId Container id related to the recovery process.
     */
    public static void recoveryCompleted(long duration, int containerId) {
        DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.CONTAINER_RECOVERY_TIME, duration, containerTag(containerId));
    }

    //endregion
}
