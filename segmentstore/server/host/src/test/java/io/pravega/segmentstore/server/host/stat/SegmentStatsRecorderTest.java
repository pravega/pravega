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
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.Getter;
import lombok.val;
import org.junit.Test;

import static io.pravega.shared.MetricsTags.segmentTags;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentStatsRecorderTest extends ThreadPooledTestSuite {
    private static final String STREAM_SEGMENT_NAME = "scope/stream/0";
    private static final String[] SEGMENT_TAGS = segmentTags(STREAM_SEGMENT_NAME);

    protected int getThreadPoolSize() {
        return 3;
    }

    @Test(timeout = 20000)
    public void testRecordTraffic() {
        // Do not mock metrics here. We are making a huge number of invocations and mockito will record every single one
        // of them, possibly causing OOMs.
        @Cleanup
        val context = new TestContext(Duration.ofSeconds(10), false);
        context.statsRecorder.createSegment(STREAM_SEGMENT_NAME, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 10, Duration.ofSeconds(1));
        assertEquals(0, (int) context.statsRecorder.getIfPresent(STREAM_SEGMENT_NAME).getTwoMinuteRate());

        // record for over 5 seconds
        long startTime = System.currentTimeMillis();
        // after 10 seconds we should have written ~100 events.
        // Which means 2 minute rate at this point is 100 / 120 ~= 0.4 events per second
        val elapsed = Duration.ofSeconds(1);
        while (System.currentTimeMillis() - startTime < Duration.ofSeconds(6).toMillis()) {
            for (int i = 0; i < 11; i++) {
                context.statsRecorder.recordAppend(STREAM_SEGMENT_NAME, 0, 1, elapsed);
            }
        }
        AssertExtensions.assertGreaterThan("", 0, (long) context.statsRecorder.getIfPresent(STREAM_SEGMENT_NAME).getTwoMinuteRate());
    }

    @Test(timeout = 10000)
    public void testExpireSegment() throws Exception {
        @Cleanup
        val context = new TestContext(Duration.ofSeconds(2), true);
        context.statsRecorder.createSegment(STREAM_SEGMENT_NAME, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 10, Duration.ofSeconds(1));

        assertNotNull(context.statsRecorder.getIfPresent(STREAM_SEGMENT_NAME));
        Thread.sleep(2500);

        // Verify that segment has been removed from the cache
        assertNull(context.statsRecorder.getIfPresent(STREAM_SEGMENT_NAME));

        // this should result in asynchronous loading of STREAM_SEGMENT_NAME
        context.statsRecorder.recordAppend(STREAM_SEGMENT_NAME, 0, 1, Duration.ofSeconds(2));
        context.getLoadAsyncCompletion().get(10000, TimeUnit.MILLISECONDS);
        assertNotNull(context.statsRecorder.getIfPresent(STREAM_SEGMENT_NAME));
    }

    @Test(timeout = 10000)
    public void testMetrics() {
        @Cleanup
        val context = new TestContext(Duration.ofSeconds(2), true);
        val elapsed = Duration.ofSeconds(1);

        // Create Segment metrics.
        context.statsRecorder.createSegment(STREAM_SEGMENT_NAME, ScalingPolicy.ScaleType.BY_RATE_IN_KBYTES_PER_SEC.getValue(), 2, elapsed);
        verify(context.createStreamSegment).reportSuccessEvent(eq(elapsed));

        // Append metrics non-txn.
        context.statsRecorder.recordAppend(STREAM_SEGMENT_NAME, 123L, 2, elapsed);
        verify(context.dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.SEGMENT_WRITE_BYTES), 123L);
        verify(context.dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.SEGMENT_WRITE_EVENTS), 2);
        verify(context.dynamicLogger).incCounterValue(MetricsNames.SEGMENT_WRITE_BYTES, 123L, SEGMENT_TAGS);
        verify(context.dynamicLogger).incCounterValue(MetricsNames.SEGMENT_WRITE_EVENTS, 2, SEGMENT_TAGS);

        // Append the 1st metrics txn.
        val txnName = StreamSegmentNameUtils.getTransactionNameFromId(STREAM_SEGMENT_NAME, UUID.randomUUID());
        context.statsRecorder.recordAppend(txnName, 321L, 5, elapsed);
        verify(context.dynamicLogger, times(1)).incCounterValue(MetricsNames.globalMetricName(MetricsNames.SEGMENT_WRITE_BYTES), 123L);
        verify(context.dynamicLogger, times(1)).incCounterValue(MetricsNames.globalMetricName(MetricsNames.SEGMENT_WRITE_BYTES), 321L);
        verify(context.dynamicLogger, times(1)).incCounterValue(MetricsNames.globalMetricName(MetricsNames.SEGMENT_WRITE_EVENTS), 2);
        verify(context.dynamicLogger, times(1)).incCounterValue(MetricsNames.globalMetricName(MetricsNames.SEGMENT_WRITE_EVENTS), 5);
        verify(context.dynamicLogger, never()).incCounterValue(MetricsNames.SEGMENT_WRITE_BYTES, 321L, segmentTags(txnName));
        verify(context.dynamicLogger, never()).incCounterValue(MetricsNames.SEGMENT_WRITE_EVENTS, 5, segmentTags(txnName));

        // Delete the 1st txn segment, this shouldn't affect the parent segment
        context.statsRecorder.deleteSegment(txnName);

        // Append the 2nd metrics txn.
        val txnName2 = StreamSegmentNameUtils.getTransactionNameFromId(STREAM_SEGMENT_NAME, UUID.randomUUID());
        context.statsRecorder.recordAppend(txnName2, 321L, 5, elapsed);

        // Seal the 2nd txn segment, this shouldn't affect the parent segment
        context.statsRecorder.sealSegment(txnName2);

        // Read metrics.
        context.statsRecorder.read(STREAM_SEGMENT_NAME, 123);
        verify(context.dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.SEGMENT_READ_BYTES), 123);
        verify(context.dynamicLogger).incCounterValue(MetricsNames.SEGMENT_READ_BYTES, 123, SEGMENT_TAGS);

        context.statsRecorder.readComplete(elapsed);
        verify(context.readStreamSegment).reportSuccessEvent(eq(elapsed));

        // Seal metrics.
        context.statsRecorder.sealSegment(STREAM_SEGMENT_NAME);
        verify(context.dynamicLogger).freezeCounter(MetricsNames.SEGMENT_WRITE_BYTES, SEGMENT_TAGS);
        verify(context.dynamicLogger).freezeCounter(MetricsNames.SEGMENT_WRITE_EVENTS, SEGMENT_TAGS);

        // Merge metrics.
        context.statsRecorder.merge(STREAM_SEGMENT_NAME, 123L, 2, 234L);
        verify(context.dynamicLogger, times(2)).incCounterValue(MetricsNames.SEGMENT_WRITE_BYTES, 123L, SEGMENT_TAGS);
        verify(context.dynamicLogger, times(2)).incCounterValue(MetricsNames.SEGMENT_WRITE_EVENTS, 2, SEGMENT_TAGS);

        // Delete metrics.
        context.statsRecorder.deleteSegment(STREAM_SEGMENT_NAME);
        verify(context.dynamicLogger, times(2)).freezeCounter(MetricsNames.SEGMENT_WRITE_BYTES, SEGMENT_TAGS);
        verify(context.dynamicLogger, times(2)).freezeCounter(MetricsNames.SEGMENT_WRITE_EVENTS, SEGMENT_TAGS);
        verify(context.dynamicLogger).freezeCounter(MetricsNames.SEGMENT_READ_BYTES, SEGMENT_TAGS);
    }

    private class TestContext implements AutoCloseable {
        final OpStatsLogger createStreamSegment;
        final OpStatsLogger readStreamSegment;
        final OpStatsLogger writeStreamSegment;
        final DynamicLogger dynamicLogger;
        final SegmentStatsRecorderImpl statsRecorder;

        TestContext(Duration expiryTime, boolean mockMetrics) {
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
            val reportingDuration = Duration.ofSeconds(10000);
            if (mockMetrics) {
                dynamicLogger = mock(DynamicLogger.class);
                createStreamSegment = mock(OpStatsLogger.class);
                readStreamSegment = mock(OpStatsLogger.class);
                writeStreamSegment = mock(OpStatsLogger.class);
                statsRecorder = new TestRecorder(processor, store, reportingDuration, expiryTime, executorService(),
                        dynamicLogger, createStreamSegment, readStreamSegment, writeStreamSegment);
            } else {
                dynamicLogger = null;
                createStreamSegment = null;
                readStreamSegment = null;
                writeStreamSegment = null;
                statsRecorder = new SegmentStatsRecorderImpl(processor, store, reportingDuration, expiryTime, executorService());
            }
        }

        public CompletableFuture<Void> getLoadAsyncCompletion() {
            if (statsRecorder instanceof TestRecorder) {
                return ((TestRecorder) statsRecorder).loadAsyncCompletion;
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public void close() {
            statsRecorder.close();
        }
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

        TestRecorder(AutoScaleProcessor reporter, StreamSegmentStore store, Duration reportingDuration, Duration expiryDuration,
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
