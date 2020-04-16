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

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.shared.MetricsTags.containerTag;
import static io.pravega.shared.MetricsTags.throttlerTag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for the {@link SegmentStoreMetrics} class.
 */
@Slf4j
public class SegmentStoreMetricsTests {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

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
        int containerId = 0;
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

        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_APPEND_COUNT, containerTag(containerId)));
        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_APPEND_OFFSET_COUNT, containerTag(containerId)));
        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_CREATE_SEGMENT_COUNT, containerTag(containerId)));
        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_DELETE_SEGMENT_COUNT, containerTag(containerId)));
        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_MERGE_SEGMENT_COUNT, containerTag(containerId)));
        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_UPDATE_ATTRIBUTES_COUNT, containerTag(containerId)));
        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_GET_ATTRIBUTES_COUNT, containerTag(containerId)));
        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_READ_COUNT, containerTag(containerId)));
        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_GET_INFO_COUNT, containerTag(containerId)));
        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_SEAL_COUNT, containerTag(containerId)));
        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_TRUNCATE_COUNT, containerTag(containerId)));

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

}
