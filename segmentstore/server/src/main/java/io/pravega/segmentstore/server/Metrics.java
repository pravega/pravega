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
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * General Metrics for the SegmentStore.
 */
public final class Metrics {
    private static final DynamicLogger LOGGER = MetricsProvider.getDynamicLogger();

    public static void cacheStats(long totalBytes, int generationSpread) {
        LOGGER.reportGaugeValue(MetricsNames.CACHE_TOTAL_SIZE_BYTES, totalBytes);
        LOGGER.reportGaugeValue(MetricsNames.CACHE_GENERATION_SPREAD, generationSpread);
    }

    public final static class ThreadPool implements AutoCloseable {
        private final ScheduledExecutorService executor;
        private final ScheduledFuture<?> reporter;

        public ThreadPool(ScheduledExecutorService executor) {
            this.executor = Preconditions.checkNotNull(executor, "executor");
            this.reporter = executor.scheduleWithFixedDelay(this::report, 1000, 1000, TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() {
            this.reporter.cancel(true);
        }

        private void report() {
            ExecutorServiceHelpers.Snapshot s = ExecutorServiceHelpers.getSnapshot(this.executor);
            if (s != null) {
                LOGGER.reportGaugeValue(MetricsNames.THREAD_POOL_QUEUE_SIZE, s.getQueueSize());
                LOGGER.reportGaugeValue(MetricsNames.THREAD_POOL_USED_THREADS, s.getActiveThreadCount());
            }
        }
    }

    /**
     * OperationProcessor metrics.
     */
    public final static class OperationProcessor {
        private final String operationQueueSize;
        private final String operationsInFlight;
        private final String operationQueueWaitTime;
        private final String operationProcessorDelay;

        public OperationProcessor(int containerId) {
            this.operationQueueSize = MetricsNames.nameFromContainer(MetricsNames.OPERATION_QUEUE_SIZE, containerId);
            this.operationsInFlight = MetricsNames.nameFromContainer(MetricsNames.OPERATION_PROCESSOR_IN_FLIGHT, containerId);
            this.operationQueueWaitTime = MetricsNames.nameFromContainer(MetricsNames.OPERATION_QUEUE_WAIT_TIME, containerId);
            this.operationProcessorDelay = MetricsNames.nameFromContainer(MetricsNames.OPERATION_PROCESSOR_DELAY_MILLIS, containerId);
        }

        public void report(int queueSize, int inFlightCount, long queueWaitTimeMillis) {
            LOGGER.reportGaugeValue(this.operationQueueSize, queueSize);
            LOGGER.reportGaugeValue(this.operationsInFlight, inFlightCount);
            LOGGER.reportGaugeValue(this.operationQueueWaitTime, queueWaitTimeMillis);
        }

        public void delay(int millis) {
            LOGGER.reportGaugeValue(this.operationProcessorDelay, millis);
        }
    }

    /**
     * ContainerMetadata metrics.
     */
    public final static class Metadata {
        private final String activeSegmentCount;

        public Metadata(int containerId) {
            this.activeSegmentCount = MetricsNames.nameFromContainer(MetricsNames.ACTIVE_SEGMENT_COUNT, containerId);
        }

        public void segmentCount(int count) {
            LOGGER.reportGaugeValue(this.activeSegmentCount, count);
        }
    }
}
