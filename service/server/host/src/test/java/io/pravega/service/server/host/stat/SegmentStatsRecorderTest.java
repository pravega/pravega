/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.service.server.host.stat;

import io.pravega.common.netty.WireCommands;
import io.pravega.common.util.ImmutableDate;
import io.pravega.service.contracts.Attributes;
import io.pravega.service.contracts.SegmentProperties;
import io.pravega.service.contracts.StreamSegmentStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentStatsRecorderTest {
    private static final String STREAM_SEGMENT_NAME = "test/test/0";

    private SegmentStatsRecorderImpl statsRecorder;
    private final CompletableFuture<Void> latch = new CompletableFuture<>();
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private ScheduledExecutorService maintenanceExecutor = Executors.newSingleThreadScheduledExecutor();

    @Before
    public void setup() {
        AutoScaleProcessor processor = mock(AutoScaleProcessor.class);
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        CompletableFuture<SegmentProperties> toBeReturned = CompletableFuture.completedFuture(new SegmentProperties() {
            @Override
            public String getName() {
                return STREAM_SEGMENT_NAME;
            }

            @Override
            public boolean isSealed() {
                return false;
            }

            @Override
            public boolean isDeleted() {
                return false;
            }

            @Override
            public ImmutableDate getLastModified() {
                return null;
            }

            @Override
            public long getLength() {
                return 0;
            }

            @Override
            public Map<UUID, Long> getAttributes() {
                Map<UUID, Long> map = new HashMap<>();
                map.put(Attributes.SCALE_POLICY_TYPE, 0L);
                map.put(Attributes.SCALE_POLICY_RATE, 10L);
                latch.complete(null);
                return map;
            }
        });

        when(store.getStreamSegmentInfo(STREAM_SEGMENT_NAME, false, Duration.ofMinutes(1))).thenReturn(toBeReturned);

        statsRecorder = new SegmentStatsRecorderImpl(processor, store, 10000,
                2, TimeUnit.SECONDS, executor, maintenanceExecutor);
    }

    @After
    public void cleanup() {
        executor.shutdown();
        maintenanceExecutor.shutdown();
    }

    @Test(timeout = 10000)
    public void testRecordTraffic() {
        statsRecorder.createSegment(STREAM_SEGMENT_NAME, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 10);

        assertTrue(statsRecorder.getIfPresent(STREAM_SEGMENT_NAME).getTwoMinuteRate() == 0);
        // record for over 5 seconds
        long startTime = System.currentTimeMillis();
        // after 10 seconds we should have written ~100 events.
        // Which means 2 minute rate at this point is 100 / 120 ~= 0.4 events per second
        while (System.currentTimeMillis() - startTime < Duration.ofSeconds(6).toMillis()) {
            for (int i = 0; i < 11; i++) {
                statsRecorder.record(STREAM_SEGMENT_NAME, 0, 1);
            }
        }
        assertTrue(statsRecorder.getIfPresent(STREAM_SEGMENT_NAME).getTwoMinuteRate() > 0);
    }

    @Test(timeout = 10000)
    public void testExpireSegment() throws InterruptedException, ExecutionException {
        statsRecorder.createSegment(STREAM_SEGMENT_NAME, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 10);

        assertTrue(statsRecorder.getIfPresent(STREAM_SEGMENT_NAME) != null);
        ScheduledFuture future = maintenanceExecutor.schedule(() -> {
        }, 2500, TimeUnit.MILLISECONDS);
        future.get();
        // Verify that segment has been removed from the cache

        assertTrue(statsRecorder.getIfPresent(STREAM_SEGMENT_NAME) == null);

        // this should result in asynchronous loading of STREAM_SEGMENT_NAME
        statsRecorder.record(STREAM_SEGMENT_NAME, 0, 1);
        latch.get();
        future = maintenanceExecutor.schedule(() -> {
        }, 500, TimeUnit.MILLISECONDS);
        future.get();
        assertTrue(statsRecorder.getIfPresent(STREAM_SEGMENT_NAME) != null);
    }
}
