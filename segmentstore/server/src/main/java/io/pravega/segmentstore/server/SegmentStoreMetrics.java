/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.logs.operations.CompletableOperation;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AccessLevel;
import lombok.Getter;

import static io.pravega.shared.MetricsTags.containerTag;

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

    /**
     * Global metric to publish the time elapsed for the recovery of Segment Store containers.
     */
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final static AtomicReference<OpStatsLogger> RECOVERY_TIMES = new AtomicReference<>();

    static {
        RECOVERY_TIMES.set(STATS_LOGGER.createStats(MetricsNames.CONTAINER_RECOVERY_TIME));
    }

    //region CacheManager

    /**
     * CacheManager metrics.
     */
    public final static class CacheManager implements AutoCloseable {
        private final OpStatsLogger generationSpread = STATS_LOGGER.createStats(MetricsNames.CACHE_GENERATION_SPREAD);

        public void report(long totalBytes, int generationSpread) {
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.CACHE_TOTAL_SIZE_BYTES, totalBytes);
            this.generationSpread.reportSuccessValue(generationSpread);
        }

        @Override
        public void close()  {
            this.generationSpread.close();
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
         * Amount of time the OperationProcessor delays between calls to processOperations() when there is significant
         * Tier1 backup.
         */
        private final OpStatsLogger operationProcessorDelay;

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
        private final String[] containerTag;

        public OperationProcessor(int containerId) {
            this.containerTag = containerTag(containerId);
            this.operationQueueSize = STATS_LOGGER.createStats(MetricsNames.OPERATION_QUEUE_SIZE, this.containerTag);
            this.operationsInFlight = STATS_LOGGER.createStats(MetricsNames.OPERATION_PROCESSOR_IN_FLIGHT, this.containerTag);
            this.operationQueueWaitTime = STATS_LOGGER.createStats(MetricsNames.OPERATION_QUEUE_WAIT_TIME, this.containerTag);
            this.operationProcessorDelay = STATS_LOGGER.createStats(MetricsNames.OPERATION_PROCESSOR_DELAY_MILLIS, this.containerTag);
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
            this.operationProcessorDelay.close();
            this.operationCommitLatency.close();
            this.operationLatency.close();
            this.memoryCommitLatency.close();
            this.memoryCommitCount.close();
            this.processOperationsLatency.close();
            this.processOperationsBatchSize.close();
        }

        public void currentState(int queueSize, int inFlightCount) {
            this.operationQueueSize.reportSuccessValue(queueSize);
            this.operationsInFlight.reportSuccessValue(inFlightCount);
        }

        public void processingDelay(int millis) {
            this.operationProcessorDelay.reportSuccessValue(millis);
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
    public final static class Container {
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
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_GET_ATTRIBUTES_COUNT, 1, containerTag);
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
    }

    //endregion

    //region RecoveryProcessor

    /**
     * RecoveryProcessor metrics.
     *
     * @param duration Time taken for a Segment Store instance to perform the recovery of containers.
     */
    public static void recoveryCompleted(long duration) {
        RECOVERY_TIMES.get().reportSuccessValue(duration);
    }

    //endregion
}
