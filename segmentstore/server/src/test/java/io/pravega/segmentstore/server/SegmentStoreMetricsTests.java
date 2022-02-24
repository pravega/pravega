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

import io.pravega.common.AbstractTimer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.logs.operations.CompletableOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.storage.cache.CacheState;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SerializedClassRunner;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.pravega.shared.MetricsTags.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for the {@link SegmentStoreMetrics} class.
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
public class SegmentStoreMetricsTests {
    @Before
    public void setUp() {
        MetricsProvider.initialize(MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .build());
        MetricsProvider.getMetricsProvider().startWithoutExporting();
    }

    /**
     * Verify that the Segment Store recovery times are properly reported.
     */
    @Test
    public void testContainerRecoveryDurationMetric() {
        int containerId = new Random().nextInt(Integer.MAX_VALUE);
        assertNull(MetricRegistryUtils.getGauge(MetricsNames.CONTAINER_RECOVERY_TIME, containerTag(containerId)));
        SegmentStoreMetrics.recoveryCompleted(1000, containerId);
        assertEquals(1000, (long) MetricRegistryUtils.getGauge(MetricsNames.CONTAINER_RECOVERY_TIME, containerTag(containerId)).value());
        SegmentStoreMetrics.recoveryCompleted(500, containerId);
        assertEquals(500, (long) MetricRegistryUtils.getGauge(MetricsNames.CONTAINER_RECOVERY_TIME, containerTag(containerId)).value());
    }

    @Test
    public void testContainerMetrics() {
        int containerId = new Random().nextInt(Integer.MAX_VALUE);
        @Cleanup
        SegmentStoreMetrics.Container containerMetrics = new SegmentStoreMetrics.Container(containerId);
        containerMetrics.createSegment();
        containerMetrics.deleteSegment();
        containerMetrics.append();
        containerMetrics.appendWithOffset();
        containerMetrics.updateAttributes();
        containerMetrics.getAttributes();
        containerMetrics.read();
        containerMetrics.getInfo();
        containerMetrics.mergeSegment();
        containerMetrics.seal();
        containerMetrics.truncate();

        assertEquals(1, MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_APPEND_COUNT, containerTag(containerId)).count());
        assertEquals(1, MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_APPEND_OFFSET_COUNT, containerTag(containerId)).count());
        assertEquals(1, MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_CREATE_SEGMENT_COUNT, containerTag(containerId)).count());
        assertEquals(1, MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_DELETE_SEGMENT_COUNT, containerTag(containerId)).count());
        assertEquals(1, MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_MERGE_SEGMENT_COUNT, containerTag(containerId)).count());
        assertEquals(1, MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_UPDATE_ATTRIBUTES_COUNT, containerTag(containerId)).count());
        assertEquals(1, MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_GET_ATTRIBUTES_COUNT, containerTag(containerId)).count());
        assertEquals(1, MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_READ_COUNT, containerTag(containerId)).count());
        assertEquals(1, MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_GET_INFO_COUNT, containerTag(containerId)).count());
        assertEquals(1, MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_SEAL_COUNT, containerTag(containerId)).count());
        assertEquals(1, MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_TRUNCATE_COUNT, containerTag(containerId)).count());

        containerMetrics.close();

        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_APPEND_COUNT, containerTag(containerId)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_APPEND_OFFSET_COUNT, containerTag(containerId)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_CREATE_SEGMENT_COUNT, containerTag(containerId)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_DELETE_SEGMENT_COUNT, containerTag(containerId)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_MERGE_SEGMENT_COUNT, containerTag(containerId)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_UPDATE_ATTRIBUTES_COUNT, containerTag(containerId)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_GET_ATTRIBUTES_COUNT, containerTag(containerId)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_READ_COUNT, containerTag(containerId)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_GET_INFO_COUNT, containerTag(containerId)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_SEAL_COUNT, containerTag(containerId)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_TRUNCATE_COUNT, containerTag(containerId)));
    }

    @Test
    public void testStorageWriterMetrics() {
        final int readCount = 123;
        final Duration iterationElapsed = Duration.ofMillis(1234);
        final long flushedBytes = 1;
        final long mergedBytes = 2;
        final int flushedAttributes = 3;
        final Duration flushDuration = Duration.ofMillis(12345);
        final int containerId = new Random().nextInt(Integer.MAX_VALUE);
        final String[] containerTag = containerTag(containerId);
        @Cleanup
        SegmentStoreMetrics.StorageWriter m = new SegmentStoreMetrics.StorageWriter(containerId);

        m.readComplete(readCount);
        assertEquals(readCount, (long) MetricRegistryUtils.getCounter(MetricsNames.STORAGE_WRITER_READ_COUNT, containerTag).count());

        m.flushComplete(flushedBytes, mergedBytes, flushedAttributes, flushDuration);
        assertEquals(flushedBytes, (long) MetricRegistryUtils.getCounter(MetricsNames.STORAGE_WRITER_FLUSHED_BYTES, containerTag).count());
        assertEquals(mergedBytes, (long) MetricRegistryUtils.getCounter(MetricsNames.STORAGE_WRITER_MERGED_BYTES, containerTag).count());
        assertEquals(flushedAttributes, (int) MetricRegistryUtils.getCounter(MetricsNames.STORAGE_WRITER_FLUSHED_ATTRIBUTES, containerTag).count());
        assertEquals(flushDuration.toMillis(), (int) MetricRegistryUtils.getTimer(MetricsNames.STORAGE_WRITER_FLUSH_ELAPSED, containerTag).mean(TimeUnit.MILLISECONDS));

        m.iterationComplete(iterationElapsed);
        assertEquals(iterationElapsed.toMillis(), (int) MetricRegistryUtils.getTimer(MetricsNames.STORAGE_WRITER_ITERATION_ELAPSED, containerTag).mean(TimeUnit.MILLISECONDS));

        m.close();

        assertNull(MetricRegistryUtils.getCounter(MetricsNames.STORAGE_WRITER_READ_COUNT, containerTag));
        assertNull(MetricRegistryUtils.getCounter(MetricsNames.STORAGE_WRITER_FLUSHED_BYTES, containerTag));
        assertNull(MetricRegistryUtils.getCounter(MetricsNames.STORAGE_WRITER_MERGED_BYTES, containerTag));
        assertNull(MetricRegistryUtils.getCounter(MetricsNames.STORAGE_WRITER_FLUSHED_ATTRIBUTES, containerTag));
        assertNull(MetricRegistryUtils.getTimer(MetricsNames.STORAGE_WRITER_FLUSH_ELAPSED, containerTag));
        assertNull(MetricRegistryUtils.getTimer(MetricsNames.STORAGE_WRITER_ITERATION_ELAPSED, containerTag));
    }

    @Test
    public void testThrottlerMetrics() {
        final int delay = 100;
        final int containerId = new Random().nextInt(Integer.MAX_VALUE);

        @Cleanup
        SegmentStoreMetrics.OperationProcessor op = new SegmentStoreMetrics.OperationProcessor(containerId);

        op.processingDelay(delay, "DurableDataLog");
        assertEquals(delay, (int) MetricRegistryUtils.getGauge(MetricsNames.OPERATION_PROCESSOR_DELAY_MILLIS, throttlerTag(containerId, "DurableDataLog")).value());

        op.processingDelay(delay, "Cache");
        assertEquals(delay, (int) MetricRegistryUtils.getGauge(MetricsNames.OPERATION_PROCESSOR_DELAY_MILLIS, throttlerTag(containerId, "Cache")).value());

        op.processingDelay(delay, "Batching");
        assertEquals(delay, (int) MetricRegistryUtils.getGauge(MetricsNames.OPERATION_PROCESSOR_DELAY_MILLIS, throttlerTag(containerId, "Batching")).value());
        op.processingDelay(delay * delay, "Batching");
        assertEquals(delay * delay, (int) MetricRegistryUtils.getGauge(MetricsNames.OPERATION_PROCESSOR_DELAY_MILLIS, throttlerTag(containerId, "Batching")).value());
    }

    @Test
    public void testCacheManagerMetrics() {
        long storedBytes = 10000;
        long usedBytes = 1000;
        long allocatedBytes = 100;
        int generationSpread = 10;
        long managerIterationDuration = 1;

        @Cleanup
        SegmentStoreMetrics.CacheManager cache = new SegmentStoreMetrics.CacheManager();
        cache.report(new CacheState(storedBytes, usedBytes, 0, allocatedBytes, storedBytes), generationSpread, managerIterationDuration);

        assertEquals(storedBytes, (long) MetricRegistryUtils.getGauge(MetricsNames.CACHE_STORED_SIZE_BYTES).value());
        assertEquals(usedBytes, (long) MetricRegistryUtils.getGauge(MetricsNames.CACHE_USED_SIZE_BYTES).value());
        assertEquals(allocatedBytes, (long) MetricRegistryUtils.getGauge(MetricsNames.CACHE_ALLOC_SIZE_BYTES).value());
        assertEquals(generationSpread, (int) MetricRegistryUtils.getGauge(MetricsNames.CACHE_GENERATION_SPREAD).value());
        assertEquals(managerIterationDuration, (long) MetricRegistryUtils.getTimer(MetricsNames.CACHE_MANAGER_ITERATION_DURATION).mean(TimeUnit.MILLISECONDS));

        cache.close();

        assertNull(MetricRegistryUtils.getGauge(MetricsNames.CACHE_STORED_SIZE_BYTES));
        assertNull(MetricRegistryUtils.getGauge(MetricsNames.CACHE_USED_SIZE_BYTES));
        assertNull(MetricRegistryUtils.getGauge(MetricsNames.CACHE_ALLOC_SIZE_BYTES));
        assertNull(MetricRegistryUtils.getGauge(MetricsNames.CACHE_GENERATION_SPREAD));
        assertNull(MetricRegistryUtils.getTimer(MetricsNames.CACHE_MANAGER_ITERATION_DURATION));
    }

    @Test
    public void testThreadPoolMetrics() throws Exception {
        @Cleanup("shutdown")
        ScheduledExecutorService coreExecutor = ExecutorServiceHelpers.newScheduledThreadPool(30, "core", Thread.NORM_PRIORITY);
        @Cleanup("shutdown")
        ScheduledExecutorService storageExecutor = ExecutorServiceHelpers.newScheduledThreadPool(30, "storage-io", Thread.NORM_PRIORITY);

        @Cleanup
        SegmentStoreMetrics.ThreadPool pool = new SegmentStoreMetrics.ThreadPool(coreExecutor, storageExecutor);

        AssertExtensions.assertEventuallyEquals(true, () -> MetricRegistryUtils.getTimer(MetricsNames.THREAD_POOL_QUEUE_SIZE).count() == 1, 2000);
        AssertExtensions.assertEventuallyEquals(true, () -> MetricRegistryUtils.getTimer(MetricsNames.STORAGE_THREAD_POOL_ACTIVE_THREADS).count() == 1, 2000);
        AssertExtensions.assertEventuallyEquals(true, () -> MetricRegistryUtils.getTimer(MetricsNames.STORAGE_THREAD_POOL_QUEUE_SIZE).count() == 1, 2000);
        AssertExtensions.assertEventuallyEquals(true, () -> MetricRegistryUtils.getTimer(MetricsNames.STORAGE_THREAD_POOL_ACTIVE_THREADS).count() == 1, 2000);
    }

    @Test
    public void testOperationProcessorMetrics() throws Exception {
        int containerId = 1;
        final String[] containerTag = containerTag(containerId);
        @Cleanup
        val op = new SegmentStoreMetrics.OperationProcessor(containerId);
        val ops = Arrays.<CompletableOperation>asList(
                new TestCompletableOperation(10),
                new TestCompletableOperation(20),
                new TestCompletableOperation(30));
        op.operationsCompleted(Collections.singletonList(ops), Duration.ZERO);
        val opf = Arrays.<CompletableOperation>asList(
                new TestCompletableOperation(20),
                new TestCompletableOperation(30));
        op.operationsFailed(opf);
        assertEquals(20, (int) MetricRegistryUtils.getTimer(MetricsNames.OPERATION_LATENCY, containerTag).totalTime(TimeUnit.MILLISECONDS));
        op.reportOperationLogSize(1000, containerId);
        AssertExtensions.assertEventuallyEquals(true, () -> MetricRegistryUtils.getGauge(MetricsNames.OPERATION_LOG_SIZE, containerTag(containerId)).value() == 1000, 2000);
        op.close();
        assertNull(MetricRegistryUtils.getGauge(MetricsNames.OPERATION_LOG_SIZE, containerTag));
    }

    @Test
    public void testTableSegmentCreditsMetrics() throws Exception {
        String segmentName = "_system/test/segment";
        SegmentStoreMetrics.tableSegmentUsedCredits(segmentName, 100);
        AssertExtensions.assertEventuallyEquals(true, () -> MetricRegistryUtils.getGauge(MetricsNames.TABLE_SEGMENT_USED_CREDITS, TAG_SEGMENT, segmentName).value() == 100, 2000);
    }

    private static class TestCompletableOperation extends CompletableOperation {
        private final ManualTimer timer;

        public TestCompletableOperation(long millis) {
            super(new MetadataCheckpointOperation(), OperationPriority.Normal, new CompletableFuture<>());
            this.timer = new ManualTimer();
            this.timer.setElapsedMillis(millis);
        }

        @Override
        public AbstractTimer getTimer() {
            return timer;
        }
    }

}
