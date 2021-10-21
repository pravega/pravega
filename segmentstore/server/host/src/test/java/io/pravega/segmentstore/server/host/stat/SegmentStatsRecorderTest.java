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
package io.pravega.segmentstore.server.host.stat;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.NameUtils;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SerializedClassRunner;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.pravega.shared.MetricsNames.SEGMENT_READ_BYTES;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_BYTES;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_EVENTS;
import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsTags.segmentTags;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(SerializedClassRunner.class)
public class SegmentStatsRecorderTest extends ThreadPooledTestSuite {
    private static final String STREAM_SEGMENT_NAME_BASE = "scope/stream/";
    private static final long NO_COUNTER_VALUE = Long.MIN_VALUE;

    @Override
    protected int getThreadPoolSize() {
        // Use an InlineExecutor to help with the async operations (they will be executed synchronously).
        return 0;
    }

    private String getStreamSegmentName() {
        return STREAM_SEGMENT_NAME_BASE + UUID.randomUUID().getLeastSignificantBits();
    }

    @Before
    public void setUp() {
        MetricsProvider.initialize(MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .build());
        MetricsProvider.getMetricsProvider().startWithoutExporting();
    }

    @Test(timeout = 20000)
    public void testRecordTraffic() {
        val segmentName = getStreamSegmentName();
        // Do not mock metrics here. We are making a huge number of invocations and mockito will record every single one
        // of them, possibly causing OOMs.
        @Cleanup
        val context = new TestContext(segmentName, Duration.ofSeconds(10), false);
        context.statsRecorder.createSegment(segmentName, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 10, Duration.ofSeconds(1));
        assertEquals(0, (int) context.statsRecorder.getSegmentAggregates(segmentName).getTwoMinuteRate());

        // record for over 5 seconds
        long startTime = System.currentTimeMillis();
        // after 10 seconds we should have written ~100 events.
        // Which means 2 minute rate at this point is 100 / 120 ~= 0.4 events per second
        val elapsed = Duration.ofSeconds(1);
        while (System.currentTimeMillis() - startTime < Duration.ofSeconds(6).toMillis()) {
            for (int i = 0; i < 11; i++) {
                context.statsRecorder.recordAppend(segmentName, 0, 1, elapsed);
            }
        }
        AssertExtensions.assertGreaterThan("", 0, (long) context.statsRecorder.getSegmentAggregates(segmentName).getTwoMinuteRate());
    }

    @Test(timeout = 10000)
    public void testExpireSegment() throws Exception {
        val segmentName = getStreamSegmentName();
        @Cleanup
        val context = new TestContext(segmentName, Duration.ofSeconds(2), true);
        context.statsRecorder.createSegment(segmentName, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 10, Duration.ofSeconds(1));
        assertEquals(0, getCounterValue(MetricsNames.SEGMENT_WRITE_BYTES, segmentName));
        assertEquals(0, getCounterValue(MetricsNames.SEGMENT_WRITE_EVENTS, segmentName));

        assertNotNull(context.statsRecorder.getSegmentAggregates(segmentName));
        Thread.sleep(2500);

        // Verify that segment has been removed from the cache
        assertNull(context.statsRecorder.getSegmentAggregates(segmentName));
        assertEquals(NO_COUNTER_VALUE, getCounterValue(MetricsNames.SEGMENT_WRITE_BYTES, segmentName));
        assertEquals(NO_COUNTER_VALUE, getCounterValue(MetricsNames.SEGMENT_WRITE_EVENTS, segmentName));

        // this should result in asynchronous loading of STREAM_SEGMENT_NAME
        context.statsRecorder.recordAppend(segmentName, 0, 1, Duration.ofSeconds(2));
        context.getLoadAsyncCompletion().get(10000, TimeUnit.MILLISECONDS);
        assertNotNull(context.statsRecorder.getSegmentAggregates(segmentName));

        context.statsRecorder.recordAppend(segmentName, 10, 11, Duration.ofSeconds(2));
        assertEquals(10, getCounterValue(MetricsNames.SEGMENT_WRITE_BYTES, segmentName));
        assertEquals(11, getCounterValue(MetricsNames.SEGMENT_WRITE_EVENTS, segmentName));
    }

    @Test(timeout = 10000)
    public void testMetrics() {
        val segmentName = getStreamSegmentName();
        @Cleanup
        val context = new TestContext(segmentName, Duration.ofSeconds(10), false);
        val elapsed = Duration.ofSeconds(1);

        // Create Segment metrics.
        context.statsRecorder.createSegment(segmentName, ScalingPolicy.ScaleType.BY_RATE_IN_KBYTES_PER_SEC.getValue(), 2, elapsed);
        assertEquals(elapsed.toMillis(), getTimerMillis(MetricsNames.SEGMENT_CREATE_LATENCY));

        // Append metrics non-txn.
        context.statsRecorder.recordAppend(segmentName, 123L, 2, elapsed);
        assertEquals(123L, getCounterValue(globalMetricName(SEGMENT_WRITE_BYTES), null));
        assertEquals(2, getCounterValue(globalMetricName(SEGMENT_WRITE_EVENTS), null));
        assertEquals(123L, getCounterValue(MetricsNames.SEGMENT_WRITE_BYTES, segmentName));
        assertEquals(2, getCounterValue(MetricsNames.SEGMENT_WRITE_EVENTS, segmentName));

        // Append the 1st metrics txn.
        val txnName = NameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
        context.statsRecorder.recordAppend(txnName, 321L, 5, elapsed);
        assertEquals(123L + 321L, getCounterValue(globalMetricName(SEGMENT_WRITE_BYTES), null));
        assertEquals(2 + 5, getCounterValue(globalMetricName(SEGMENT_WRITE_EVENTS), null));

        // Verify the transaction metrics did not get recorded. Txn segment tags are the same as the parent segment tags.
        assertEquals(123L, getCounterValue(MetricsNames.SEGMENT_WRITE_BYTES, segmentName));
        assertEquals(2, getCounterValue(MetricsNames.SEGMENT_WRITE_EVENTS, segmentName));

        // Delete the 1st txn segment, this shouldn't affect the parent segment
        context.statsRecorder.deleteSegment(txnName);

        // Append the 2nd metrics txn.
        val txnName2 = NameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
        context.statsRecorder.recordAppend(txnName2, 321L, 5, elapsed);

        // Seal the 2nd txn segment, this shouldn't affect the parent segment
        context.statsRecorder.sealSegment(txnName2);

        // Read metrics.
        context.statsRecorder.read(segmentName, 123);
        assertEquals(123L, getCounterValue(globalMetricName(SEGMENT_READ_BYTES), null));
        assertEquals(123L, getCounterValue(SEGMENT_READ_BYTES, segmentName));

        context.statsRecorder.readComplete(elapsed);
        assertEquals(elapsed.toMillis(), getTimerMillis(MetricsNames.SEGMENT_READ_LATENCY));

        // Merge metrics.
        context.statsRecorder.merge(segmentName, 123L, 2, 234L);
        assertEquals(123L * 2, getCounterValue(MetricsNames.SEGMENT_WRITE_BYTES, segmentName));
        assertEquals(2 * 2, getCounterValue(MetricsNames.SEGMENT_WRITE_EVENTS, segmentName));

        // Seal metrics.
        context.statsRecorder.sealSegment(segmentName);
        assertEquals(NO_COUNTER_VALUE, getCounterValue(MetricsNames.SEGMENT_WRITE_BYTES, segmentName));
        assertEquals(NO_COUNTER_VALUE, getCounterValue(MetricsNames.SEGMENT_WRITE_EVENTS, segmentName));

        // Delete metrics.
        context.statsRecorder.deleteSegment(segmentName);
        assertEquals(NO_COUNTER_VALUE, getCounterValue(MetricsNames.SEGMENT_WRITE_BYTES, segmentName));
        assertEquals(NO_COUNTER_VALUE, getCounterValue(MetricsNames.SEGMENT_WRITE_EVENTS, segmentName));
        assertEquals(NO_COUNTER_VALUE, getCounterValue(MetricsNames.SEGMENT_READ_BYTES, segmentName));
    }

    @Test(timeout = 10000)
    public void testTransactionMetrics() {
        val segmentName = getStreamSegmentName();

        long dataLength = 321;
        val duration = Duration.ofMinutes(1);
        @Cleanup
        val context = new TestContext(segmentName, duration, false);

        context.statsRecorder.createSegment(segmentName, ScalingPolicy.ScaleType.BY_RATE_IN_KBYTES_PER_SEC.getValue(), 2, Duration.ofSeconds(1));
        context.statsRecorder.recordAppend(segmentName, dataLength, 5, Duration.ofSeconds(2));
        val txnName1 = NameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
        context.statsRecorder.createSegment(txnName1, ScalingPolicy.ScaleType.BY_RATE_IN_KBYTES_PER_SEC.getValue(), 2, Duration.ofSeconds(1));
        // Make sure deletions do not cause side effects.
        context.statsRecorder.deleteSegment(txnName1);
        assertEquals(dataLength, getCounterValue(SEGMENT_WRITE_BYTES, segmentName));
        // All closures of metrics happen through the SimpleCache. Asserting that an entry for `txnName1` does not exist
        // ensures that there is guaranteed to be a one-one mapping of entries to metric counters.
        assertNull(context.statsRecorder.getSegmentAggregates(txnName1));
    }


    private long getCounterValue(String counter, String segment) {
        val c = MetricRegistryUtils.getCounter(counter, segment == null ? new String[0] : segmentTags(segment));
        return c == null ? NO_COUNTER_VALUE : (long) c.count();
    }

    private long getTimerMillis(String timerName) {
        val t = MetricRegistryUtils.getTimer(timerName);
        return (long) t.totalTime(TimeUnit.MILLISECONDS);
    }

    private class TestContext implements AutoCloseable {
        final SegmentStatsRecorderImpl statsRecorder;

        TestContext(String segmentName, Duration expiryTime, boolean mockLoadAsync) {
            AutoScaleProcessor processor = mock(AutoScaleProcessor.class);
            StreamSegmentStore store = mock(StreamSegmentStore.class);
            CompletableFuture<SegmentProperties> toBeReturned = CompletableFuture.completedFuture(
                    StreamSegmentInformation.builder()
                            .name(segmentName)
                            .attributes(ImmutableMap.<AttributeId, Long>builder()
                                    .put(Attributes.SCALE_POLICY_TYPE, 0L)
                                    .put(Attributes.SCALE_POLICY_RATE, 10L).build())
                            .build());
            when(store.getStreamSegmentInfo(segmentName, Duration.ofMinutes(1))).thenReturn(toBeReturned);
            val reportingDuration = Duration.ofSeconds(10000);
            if (mockLoadAsync) {
                statsRecorder = new TestRecorder(processor, store, reportingDuration, expiryTime, executorService());
            } else {
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

        TestRecorder(AutoScaleProcessor reporter, StreamSegmentStore store, Duration reportingDuration, Duration expiryDuration,
                     ScheduledExecutorService executor) {
            super(reporter, store, reportingDuration, expiryDuration, executor);
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
