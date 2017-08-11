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
import io.pravega.common.AbstractTimer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.logs.operations.CompletableOperation;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
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
        private final OpStatsLogger operationQueueSize;
        private final OpStatsLogger operationsInFlight;
        private final OpStatsLogger operationQueueWaitTime;
        private final OpStatsLogger operationProcessorDelay;
        private final OpStatsLogger operationLatency;
        private final String operationLogSize;

        public OperationProcessor(int containerId) {
            this.operationQueueSize = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_QUEUE_SIZE, containerId));
            this.operationsInFlight = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_PROCESSOR_IN_FLIGHT, containerId));
            this.operationQueueWaitTime = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_QUEUE_WAIT_TIME, containerId));
            this.operationProcessorDelay = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_PROCESSOR_DELAY_MILLIS, containerId));
            this.operationLatency = STATS_LOGGER.createStats(MetricsNames.nameFromContainer(MetricsNames.OPERATION_LATENCY, containerId));
            this.operationLogSize = MetricsNames.nameFromContainer(MetricsNames.OPERATION_LOG_SIZE, containerId);
        }

        public void currentState(int queueSize, int inFlightCount, long queueWaitTimeMillis) {
            this.operationQueueSize.reportSuccessValue(queueSize);
            this.operationsInFlight.reportSuccessValue(inFlightCount);
            this.operationQueueWaitTime.reportSuccessValue(queueWaitTimeMillis);
        }

        public void processingDelay(int millis) {
            this.operationProcessorDelay.reportSuccessValue(millis);
        }

        public void operationLogAdd(int count) {
            DYNAMIC_LOGGER.incCounterValue(this.operationLogSize, count);
        }

        public void operationLogTruncate(int count) {
            DYNAMIC_LOGGER.incCounterValue(this.operationLogSize, -count);
        }

        public void operationLogInit() {
            DYNAMIC_LOGGER.updateCounterValue(this.operationLogSize, 0);
        }

        public void operationsCompleted(Collection<CompletableOperation> operations) {
            if (operations.size() > 0) {
                long sum = operations.stream().mapToLong(op -> op.getTimer().getElapsedNanos()).sum();
                this.operationLatency.reportSuccessValue(sum / operations.size() / AbstractTimer.NANOS_TO_MILLIS);
            }
        }

        public void operationsFailed(Collection<CompletableOperation> operations) {
            if (operations.size() > 0) {
                long sum = operations.stream().mapToLong(op -> op.getTimer().getElapsedNanos()).sum();
                this.operationLatency.reportFailValue(sum / operations.size() / AbstractTimer.NANOS_TO_MILLIS);
            }
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
}
