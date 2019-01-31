/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.stat;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.segment.ScalingPolicy;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentStatsRecorderTest extends ThreadPooledTestSuite {
    private static final String STREAM_SEGMENT_NAME = "test/test/0";
    private OpStatsLogger createStreamSegment;
    private OpStatsLogger readStreamSegment;
    private OpStatsLogger writeStreamSegment;
    private DynamicLogger dynamicLogger;
    private TestRecorder statsRecorder;

    protected int getThreadPoolSize() {
        return 3;
    }

    @Before
    public void setup() {
        AutoScaleProcessor processor = mock(AutoScaleProcessor.class);
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        CompletableFuture<SegmentProperties> toBeReturned = CompletableFuture.completedFuture(
                StreamSegmentInformation.builder()
                                        .name(STREAM_SEGMENT_NAME)
                                        .attributes(ImmutableMap.<UUID, Long>builder()
                                                .put(Attributes.SCALE_POLICY_TYPE, 0L)
                                                .put(Attributes.SCALE_POLICY_RATE, 10L).build())
                                        .build());
        when(store.getStreamSegmentInfo(STREAM_SEGMENT_NAME, Duration.ofMinutes(1))).thenReturn(toBeReturned);
        dynamicLogger = mock(DynamicLogger.class);
        createStreamSegment = mock(OpStatsLogger.class);
        readStreamSegment = mock(OpStatsLogger.class);
        writeStreamSegment = mock(OpStatsLogger.class);
        statsRecorder = new TestRecorder(processor, store, Duration.ofSeconds(10000), Duration.ofSeconds(2), executorService(),
                dynamicLogger, createStreamSegment, readStreamSegment, writeStreamSegment);
    }

    @Test(timeout = 10000)
    public void testRecordTraffic() {
        statsRecorder.createSegment(STREAM_SEGMENT_NAME, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 10, Duration.ofSeconds(1));

        assertEquals(0, (int) statsRecorder.getIfPresent(STREAM_SEGMENT_NAME).getTwoMinuteRate());
        // record for over 5 seconds
        long startTime = System.currentTimeMillis();
        // after 10 seconds we should have written ~100 events.
        // Which means 2 minute rate at this point is 100 / 120 ~= 0.4 events per second
        while (System.currentTimeMillis() - startTime < Duration.ofSeconds(6).toMillis()) {
            for (int i = 0; i < 11; i++) {
                statsRecorder.recordAppend(STREAM_SEGMENT_NAME, 0, 1, Duration.ofSeconds(i));
            }
        }
        assertTrue(statsRecorder.getIfPresent(STREAM_SEGMENT_NAME).getTwoMinuteRate() > 0);
    }

    @Test(timeout = 10000)
    public void testExpireSegment() throws Exception {
        statsRecorder.createSegment(STREAM_SEGMENT_NAME, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 10, Duration.ofSeconds(1));

        assertNotNull(statsRecorder.getIfPresent(STREAM_SEGMENT_NAME));
        Thread.sleep(2500);

        // Verify that segment has been removed from the cache
        assertNull(statsRecorder.getIfPresent(STREAM_SEGMENT_NAME));

        // this should result in asynchronous loading of STREAM_SEGMENT_NAME
        statsRecorder.recordAppend(STREAM_SEGMENT_NAME, 0, 1, Duration.ofSeconds(2));
        statsRecorder.loadAsyncCompletion.get(10000, TimeUnit.MILLISECONDS);
        assertNotNull(statsRecorder.getIfPresent(STREAM_SEGMENT_NAME));
    }

    @Test(timeout = 10000)
    public void testMetrics() {
        val elapsed = Duration.ofSeconds(1);

        // Create Segment metrics.
        statsRecorder.createSegment(STREAM_SEGMENT_NAME, ScalingPolicy.ScaleType.BY_RATE_IN_KBYTES_PER_SEC.getId(), 2, elapsed);
        verify(createStreamSegment).reportSuccessEvent(eq(elapsed));

        // Append metrics non-txn.
        statsRecorder.recordAppend(STREAM_SEGMENT_NAME, 123L, 2, elapsed);
        verify(dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.SEGMENT_WRITE_BYTES), 123L);
        verify(dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.SEGMENT_WRITE_EVENTS), 2);
        verify(dynamicLogger).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.SEGMENT_WRITE_BYTES, STREAM_SEGMENT_NAME), 123L);
        verify(dynamicLogger).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.SEGMENT_WRITE_EVENTS, STREAM_SEGMENT_NAME), 2);

        // Append metrics txn.
        val txnName = StreamSegmentNameUtils.getTransactionNameFromId(STREAM_SEGMENT_NAME, UUID.randomUUID());
        statsRecorder.recordAppend(txnName, 123L, 2, elapsed);
        verify(dynamicLogger, times(2)).incCounterValue(MetricsNames.globalMetricName(MetricsNames.SEGMENT_WRITE_BYTES), 123L);
        verify(dynamicLogger, times(2)).incCounterValue(MetricsNames.globalMetricName(MetricsNames.SEGMENT_WRITE_EVENTS), 2);
        verify(dynamicLogger, never()).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.SEGMENT_WRITE_BYTES, txnName), 123L);
        verify(dynamicLogger, never()).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.SEGMENT_WRITE_EVENTS, txnName), 2);

        // Read metrics.
        statsRecorder.read(STREAM_SEGMENT_NAME, 123);
        verify(dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.SEGMENT_READ_BYTES), 123);
        verify(dynamicLogger).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.SEGMENT_READ_BYTES, STREAM_SEGMENT_NAME), 123);

        statsRecorder.readComplete(elapsed);
        verify(readStreamSegment).reportSuccessEvent(eq(elapsed));

        // Seal metrics.
        statsRecorder.sealSegment(STREAM_SEGMENT_NAME);
        verify(dynamicLogger).freezeCounter(MetricsNames.nameFromSegment(MetricsNames.SEGMENT_WRITE_BYTES, STREAM_SEGMENT_NAME));
        verify(dynamicLogger).freezeCounter(MetricsNames.nameFromSegment(MetricsNames.SEGMENT_WRITE_EVENTS, STREAM_SEGMENT_NAME));

        // Merge metrics.
        statsRecorder.merge(STREAM_SEGMENT_NAME, 123L, 2, 234L);
        verify(dynamicLogger, times(2)).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.SEGMENT_WRITE_BYTES, STREAM_SEGMENT_NAME), 123L);
        verify(dynamicLogger, times(2)).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.SEGMENT_WRITE_EVENTS, STREAM_SEGMENT_NAME), 2);

        // Delete metrics.
        statsRecorder.deleteSegment(STREAM_SEGMENT_NAME);
        verify(dynamicLogger, times(2)).freezeCounter(MetricsNames.nameFromSegment(MetricsNames.SEGMENT_WRITE_BYTES, STREAM_SEGMENT_NAME));
        verify(dynamicLogger, times(2)).freezeCounter(MetricsNames.nameFromSegment(MetricsNames.SEGMENT_WRITE_EVENTS, STREAM_SEGMENT_NAME));
        verify(dynamicLogger).freezeCounter(MetricsNames.nameFromSegment(MetricsNames.SEGMENT_READ_BYTES, STREAM_SEGMENT_NAME));
    }

    private static class TestRecorder extends SegmentStatsRecorderImpl {
        final CompletableFuture<Void> loadAsyncCompletion;
        @Getter(AccessLevel.PROTECTED)
        private final OpStatsLogger createStreamSegment;
        @Getter(AccessLevel.PROTECTED)
        private final OpStatsLogger readStreamSegment;
        @Getter(AccessLevel.PROTECTED)
        private final OpStatsLogger writeStreamSegment;
        @Getter(AccessLevel.PROTECTED)
        private final DynamicLogger dynamicLogger;

        TestRecorder(AutoScaleProcessor reporter, StreamSegmentStore store, Duration reportingDuration, @NonNull Duration expiryDuration,
                     ScheduledExecutorService executor, DynamicLogger dynamicLogger, OpStatsLogger createStreamSegment,
                     OpStatsLogger readStreamSegment, OpStatsLogger writeStreamSegment) {
            super(reporter, store, reportingDuration, expiryDuration, executor);
            this.dynamicLogger = dynamicLogger;
            this.createStreamSegment = createStreamSegment;
            this.readStreamSegment = readStreamSegment;
            this.writeStreamSegment = writeStreamSegment;
            this.loadAsyncCompletion = new CompletableFuture<>();
        }

        @Override
        protected CompletableFuture<Void> loadAsynchronously(String streamSegmentName) {
            val r = super.loadAsynchronously(streamSegmentName);
            Futures.completeAfter(() -> r, this.loadAsyncCompletion);
            return r;
        }
    }
}
