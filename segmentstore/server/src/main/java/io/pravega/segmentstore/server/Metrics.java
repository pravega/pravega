/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.segmentstore.server;

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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * General Metrics for the SegmentStore.
 */
public final class Metrics {
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
        private final OpStatsLogger totalSize = STATS_LOGGER.createStats(MetricsNames.CACHE_TOTAL_SIZE_BYTES);
        private final OpStatsLogger generationSpread = STATS_LOGGER.createStats(MetricsNames.CACHE_GENERATION_SPREAD);

        public void report(long totalBytes, int generationSpread) {
            this.totalSize.reportSuccessValue(totalBytes);
            this.generationSpread.reportSuccessValue(generationSpread);
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
    public final static class OperationProcessor {
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
         * Amount of time the OperationProcessor delays in order to relieve Tier1 in case of back-up.
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
         * Amount of time spent ack-ing operations.
         */
        private final OpStatsLogger operationAckLatency;

        /**
         * Amount of time spent flushing into the log.
         */
        private final OpStatsLogger logFlushLatency;
        private final OpStatsLogger lockAcquireLatency;
        private final OpStatsLogger logProcessLatency;
        private final OpStatsLogger metadataCommitLatency;

        /**
         * Amount of time spent inside processOperations(Queue)
         */
        private final OpStatsLogger processOperationsLatency;
        private final String operationLogSize;

        public OperationProcessor(int containerId) {
            this.operationQueueSize = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_QUEUE_SIZE, containerId));
            this.operationsInFlight = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_PROCESSOR_IN_FLIGHT, containerId));
            this.operationQueueWaitTime = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_QUEUE_WAIT_TIME, containerId));
            this.operationProcessorDelay = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_PROCESSOR_DELAY_MILLIS, containerId));
            this.operationCommitLatency = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_COMMIT_LATENCY, containerId));
            this.operationLatency = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_LATENCY, containerId));
            this.operationAckLatency = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_ACK_LATENCY, containerId));
            this.logFlushLatency = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.LOG_FLUSH_LATENCY, containerId));
            this.lockAcquireLatency = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_COMMIT_ACQ_LOCK_LATENCY, containerId));
            this.logProcessLatency = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_COMMIT_LOG_UPDATE_LATENCY, containerId));
            this.metadataCommitLatency = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_COMMIT_METADATA_LATENCY, containerId));
            this.processOperationsLatency = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.PROCESS_OPERATIONS_LATENCY, containerId));
            this.operationLogSize = "segmentstore." + MetricsNames.nameFromContainer(MetricsNames.OPERATION_LOG_SIZE, containerId);
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

        public void operationsCommitted(int count, Duration commitElapsed, Duration ackElapsed) {
            DYNAMIC_LOGGER.incCounterValue(this.operationLogSize, count);
            this.operationCommitLatency.reportSuccessEvent(commitElapsed);
            this.operationAckLatency.reportSuccessEvent(ackElapsed);
        }

        public void lockAcquired(Duration elapsed) {
            this.lockAcquireLatency.reportSuccessEvent(elapsed);
        }

        public void logProcessed(Duration elapsed) {
            this.logProcessLatency.reportSuccessEvent(elapsed);
        }

        public void metadataCommitted(Duration elapsed) {
            this.metadataCommitLatency.reportSuccessEvent(elapsed);
        }

        public void logFlushed(Duration elapsed) {
            this.logFlushLatency.reportSuccessEvent(elapsed);
        }

        public void operationLogTruncate(int count) {
            DYNAMIC_LOGGER.incCounterValue(this.operationLogSize, -count);
        }

        public void operationLogInit() {
            DYNAMIC_LOGGER.updateCounterValue(this.operationLogSize, 0);
        }

        public void processOperationsLatency(long millis) {
            this.processOperationsLatency.reportSuccessValue(millis);
        }

        public void operationsCompleted(Collection<CompletableOperation> operations) {
            operations.forEach(o -> {
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
        private final String activeSegmentCount;

        public Metadata(int containerId) {
            this.activeSegmentCount = MetricsNames.nameFromContainer(MetricsNames.ACTIVE_SEGMENT_COUNT, containerId);
        }

        public void segmentCount(int count) {
            DYNAMIC_LOGGER.reportGaugeValue(this.activeSegmentCount, count);
        }
    }

    //endregion

    //region Container

    /**
     * StreamSegmentContainer Metrics.
     */
    public final static class Container {
        private final String appendCount;
        private final String appendOffsetCount;
        private final String updateAttributesCount;
        private final String readCount;
        private final String getInfoCount;
        private final String createSegmentCount;
        private final String deleteSegmentCount;
        private final String createTxnCount;
        private final String mergeTxnCount;
        private final String sealCount;

        public Container(int containerId) {
            this.appendCount = MetricsNames.nameFromContainer(MetricsNames.CONTAINER_APPEND_COUNT, containerId);
            this.appendOffsetCount = MetricsNames.nameFromContainer(MetricsNames.CONTAINER_APPEND_OFFSET_COUNT, containerId);
            this.updateAttributesCount = MetricsNames.nameFromContainer(MetricsNames.CONTAINER_UPDATE_ATTRIBUTES_COUNT, containerId);
            this.readCount = MetricsNames.nameFromContainer(MetricsNames.CONTAINER_READ_COUNT, containerId);
            this.getInfoCount = MetricsNames.nameFromContainer(MetricsNames.CONTAINER_GET_INFO_COUNT, containerId);
            this.createSegmentCount = MetricsNames.nameFromContainer(MetricsNames.CONTAINER_CREATE_SEGMENT_COUNT, containerId);
            this.deleteSegmentCount = MetricsNames.nameFromContainer(MetricsNames.CONTAINER_DELETE_SEGMENT_COUNT, containerId);
            this.createTxnCount = MetricsNames.nameFromContainer(MetricsNames.CONTAINER_CREATE_TXN_COUNT, containerId);
            this.mergeTxnCount = MetricsNames.nameFromContainer(MetricsNames.CONTAINER_MERGE_TXN_COUNT, containerId);
            this.sealCount = MetricsNames.nameFromContainer(MetricsNames.CONTAINER_SEAL_COUNT, containerId);
        }

        public void createSegment() {
            DYNAMIC_LOGGER.recordMeterEvents(this.createSegmentCount, 1);
        }

        public void deleteSegment() {
            DYNAMIC_LOGGER.recordMeterEvents(this.deleteSegmentCount, 1);
        }

        public void append() {
            DYNAMIC_LOGGER.recordMeterEvents(this.appendCount, 1);
        }

        public void appendWithOffset() {
            DYNAMIC_LOGGER.recordMeterEvents(this.appendOffsetCount, 1);
        }

        public void updateAttributes() {
            DYNAMIC_LOGGER.recordMeterEvents(this.updateAttributesCount, 1);
        }

        public void read() {
            DYNAMIC_LOGGER.recordMeterEvents(this.readCount, 1);
        }

        public void getInfo() {
            DYNAMIC_LOGGER.recordMeterEvents(this.getInfoCount, 1);
        }

        public void createTxn() {
            DYNAMIC_LOGGER.recordMeterEvents(this.createTxnCount, 1);
        }

        public void mergeTxn() {
            DYNAMIC_LOGGER.recordMeterEvents(this.mergeTxnCount, 1);
        }

        public void seal() {
            DYNAMIC_LOGGER.recordMeterEvents(this.sealCount, 1);
        }
    }

    //endregion
}
