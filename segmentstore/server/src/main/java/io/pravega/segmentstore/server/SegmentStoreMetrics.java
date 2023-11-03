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
package io.pravega.segmentstore.server;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.logs.operations.CompletableOperation;
import io.pravega.segmentstore.storage.cache.CacheState;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.Meter;
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
import lombok.val;

import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsTags.containerTag;
import static io.pravega.shared.MetricsTags.eventProcessorTag;
import static io.pravega.shared.MetricsTags.throttlerTag;
import static io.pravega.shared.MetricsTags.segmentTagDirect;

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
    private static final OpStatsLogger GLOBAL_OPERATION_LATENCY = STATS_LOGGER.createStats(globalMetricName(MetricsNames.OPERATION_LATENCY));

    //region CacheManager

    /**
     * CacheManager metrics.
     */
    public final static class CacheManager implements AutoCloseable {

        /**
         * The amount of time taken to complete one cycle of the CacheManager's cache policy.
         */
        private final OpStatsLogger cacheManagerIterationDuration;

        public CacheManager() {
            cacheManagerIterationDuration = STATS_LOGGER.createStats(MetricsNames.CACHE_MANAGER_ITERATION_DURATION);
        }

        public void report(CacheState snapshot, int generationSpread, long iterationDuration) {
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.CACHE_STORED_SIZE_BYTES, snapshot.getStoredBytes());
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.CACHE_USED_SIZE_BYTES, snapshot.getUsedBytes());
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.CACHE_ALLOC_SIZE_BYTES, snapshot.getAllocatedBytes());
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.CACHE_GENERATION_SPREAD, generationSpread);
            cacheManagerIterationDuration.reportSuccessValue(iterationDuration);
        }

        @Override
        public void close() {
            DYNAMIC_LOGGER.freezeGaugeValue(MetricsNames.CACHE_STORED_SIZE_BYTES);
            DYNAMIC_LOGGER.freezeGaugeValue(MetricsNames.CACHE_USED_SIZE_BYTES);
            DYNAMIC_LOGGER.freezeGaugeValue(MetricsNames.CACHE_ALLOC_SIZE_BYTES);
            DYNAMIC_LOGGER.freezeGaugeValue(MetricsNames.CACHE_GENERATION_SPREAD);
            cacheManagerIterationDuration.close();
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
        private final OpStatsLogger storageQueueSize;
        private final OpStatsLogger storageActiveThreads;
        private final ScheduledExecutorService executor;
        private final ScheduledExecutorService storageExecutor;
        private final ScheduledFuture<?> reporter;

        public ThreadPool(ScheduledExecutorService executor, ScheduledExecutorService storageExecutor) {
            this.executor = Preconditions.checkNotNull(executor, "executor");
            this.storageExecutor = Preconditions.checkNotNull(storageExecutor, "storageExecutor");
            this.queueSize = STATS_LOGGER.createStats(MetricsNames.THREAD_POOL_QUEUE_SIZE);
            this.activeThreads = STATS_LOGGER.createStats(MetricsNames.THREAD_POOL_ACTIVE_THREADS);
            this.storageQueueSize = STATS_LOGGER.createStats(MetricsNames.STORAGE_THREAD_POOL_QUEUE_SIZE);
            this.storageActiveThreads = STATS_LOGGER.createStats(MetricsNames.STORAGE_THREAD_POOL_ACTIVE_THREADS);
            this.reporter = executor.scheduleWithFixedDelay(this::report, 1000, 1000, TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() {
            this.reporter.cancel(true);
            this.queueSize.close();
            this.activeThreads.close();
            this.storageQueueSize.close();
            this.storageActiveThreads.close();
        }

        private void report() {
            ExecutorServiceHelpers.Snapshot s = ExecutorServiceHelpers.getSnapshot(this.executor);
            if (s != null) {
                this.queueSize.reportSuccessValue(s.getQueueSize());
                this.activeThreads.reportSuccessValue(s.getActiveThreadCount());
            }
            ExecutorServiceHelpers.Snapshot ss = ExecutorServiceHelpers.getSnapshot(this.storageExecutor);
            if (ss != null) {
                this.storageQueueSize.reportSuccessValue(ss.getQueueSize());
                this.storageActiveThreads.reportSuccessValue(ss.getActiveThreadCount());
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
        private final Set<String> throttlers = Collections.synchronizedSet(new HashSet<>());

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
            DYNAMIC_LOGGER.freezeGaugeValue(MetricsNames.OPERATION_LOG_SIZE, containerTag);
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

        public void processOperations(int batchSize, long millis) {
            this.processOperationsBatchSize.reportSuccessValue(batchSize);
            this.processOperationsLatency.reportSuccessValue(millis);
        }

        public void operationsCompleted(int operationCount, Duration commitElapsed) {
            this.operationCommitLatency.reportSuccessEvent(commitElapsed);
        }

        public void operationsCompleted(Collection<List<CompletableOperation>> operations, Duration commitElapsed) {
            int count = 0;
            long millis = 0;
            for (val ol : operations) {
                count += ol.size();
                for (val o : ol) {
                    millis += o.getTimer().getElapsedMillis();
                }
            }
            if (count > 0) {
                operationsCompleted(count, commitElapsed);
                millis /= count;
                this.operationLatency.reportSuccessValue(millis);
                GLOBAL_OPERATION_LATENCY.reportSuccessValue(millis);
            }
        }

        public void operationsFailed(Collection<CompletableOperation> operations) {
            if (!operations.isEmpty()) {
                long millis = operations.stream().mapToLong(o -> o.getTimer().getElapsedMillis()).sum();
                millis /= operations.size();
                this.operationLatency.reportFailValue(millis);
                GLOBAL_OPERATION_LATENCY.reportFailValue(millis);
            }
        }

        /**
         * Rerpot the operation log size for every SegmentStore Container.
         * @param logSize           Size of the operationlog to be reported.
         * @param containerId       Container owning the operationlog.
         */
        public void reportOperationLogSize(int logSize, int containerId) {
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.OPERATION_LOG_SIZE, logSize, containerTag(containerId));
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
        private final Meter createSegment;
        private final Meter deleteSegment;
        private final Meter append;
        private final Meter appendWithOffset;
        private final Meter updateAttributes;
        private final Meter getAttributes;
        private final Meter read;
        private final Meter getInfo;
        private final Meter mergeSegment;
        private final Meter seal;
        private final Meter truncate;

        public Container(int containerId) {
            String[] containerTag = containerTag(containerId);
            this.createSegment = STATS_LOGGER.createMeter(MetricsNames.CONTAINER_CREATE_SEGMENT_COUNT, containerTag);
            this.deleteSegment = STATS_LOGGER.createMeter(MetricsNames.CONTAINER_DELETE_SEGMENT_COUNT, containerTag);
            this.append = STATS_LOGGER.createMeter(MetricsNames.CONTAINER_APPEND_COUNT, containerTag);
            this.appendWithOffset = STATS_LOGGER.createMeter(MetricsNames.CONTAINER_APPEND_OFFSET_COUNT, containerTag);
            this.updateAttributes = STATS_LOGGER.createMeter(MetricsNames.CONTAINER_UPDATE_ATTRIBUTES_COUNT, containerTag);
            this.getAttributes = STATS_LOGGER.createMeter(MetricsNames.CONTAINER_GET_ATTRIBUTES_COUNT, containerTag);
            this.read = STATS_LOGGER.createMeter(MetricsNames.CONTAINER_READ_COUNT, containerTag);
            this.getInfo = STATS_LOGGER.createMeter(MetricsNames.CONTAINER_GET_INFO_COUNT, containerTag);
            this.mergeSegment = STATS_LOGGER.createMeter(MetricsNames.CONTAINER_MERGE_SEGMENT_COUNT, containerTag);
            this.seal = STATS_LOGGER.createMeter(MetricsNames.CONTAINER_SEAL_COUNT, containerTag);
            this.truncate = STATS_LOGGER.createMeter(MetricsNames.CONTAINER_TRUNCATE_COUNT, containerTag);
        }

        public void createSegment() {
            this.createSegment.recordEvent();
        }

        public void deleteSegment() {
            this.deleteSegment.recordEvent();
        }

        public void append() {
            this.append.recordEvent();
        }

        public void appendWithOffset() {
            this.appendWithOffset.recordEvent();
        }

        public void updateAttributes() {
            this.updateAttributes.recordEvent();
        }

        public void getAttributes() {
            this.getAttributes.recordEvent();
        }

        public void read() {
            this.read.recordEvent();
        }

        public void getInfo() {
            this.getInfo.recordEvent();
        }

        public void mergeSegment() {
            this.mergeSegment.recordEvent();
        }

        public void seal() {
            this.seal.recordEvent();
        }

        public void truncate() {
            this.truncate.recordEvent();
        }

        @Override
        public void close() {
            this.createSegment.close();
            this.deleteSegment.close();
            this.append.close();
            this.appendWithOffset.close();
            this.updateAttributes.close();
            this.getAttributes.close();
            this.read.close();
            this.getInfo.close();
            this.mergeSegment.close();
            this.seal.close();
            this.truncate.close();
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

    //region ContainerEventProcessor

    /**
     * EventProcessor metrics.
     */
    public final static class EventProcessor implements AutoCloseable {

        private final OpStatsLogger processingIterationLatency;

        public EventProcessor(String processorName, int containerId) {
            String[] eventProcessorTags = eventProcessorTag(containerId, processorName);
            this.processingIterationLatency = STATS_LOGGER.createStats(MetricsNames.CONTAINER_EVENT_PROCESSOR_BATCH_LATENCY, eventProcessorTags);
        }

        public void batchProcessingLatency(long latency) {
            this.processingIterationLatency.reportSuccessValue(latency);
        }

        @Override
        public void close() {
            this.processingIterationLatency.close();
        }
    }

    /**
     * Reports the outstanding bytes for a given {@link EventProcessor}.
     *
     * @param processorName      Name for the {@link EventProcessor}.
     * @param containerId        Container id where the {@link EventProcessor} is running.
     * @param outstandingBytes   Number of outstanding bytes for the {@link EventProcessor}.
     */
    public static void outstandingEventProcessorBytes(String processorName, int containerId, long outstandingBytes) {
        DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.CONTAINER_EVENT_PROCESSOR_OUTSTANDING_BYTES, outstandingBytes,
                eventProcessorTag(containerId, processorName));
    }
    //endregion

    //region ContainerKeyIndex

    /**
     * Reports the number of credits used for a given Hash-Based Table Segment. Note that these Segments are exclusively
     * used for internal metadata storage and may have a special name format. For this reason, we report the Segment name
     * directly via the tag.
     *
     * @param segmentName Name of the Table Segment that reports the used credits.
     * @param credits Used credits for the Table Segment.
     */
    public static void tableSegmentUsedCredits(String segmentName, long credits) {
        DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.TABLE_SEGMENT_USED_CREDITS, credits, segmentTagDirect(segmentName));
    }

    //endregion
}
