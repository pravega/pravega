/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host.stat;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.service.monitor.SegmentTrafficMonitor;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class SegmentAggregatesTest {

    private static final String STREAM_SEGMENT_NAME = "stream0";

    @Test
    public void testRecorder() {
        AtomicBoolean loop = new AtomicBoolean(true);
        List<SegmentTrafficMonitor> monitors = Lists.newArrayList(new SegmentTrafficMonitor() {
            @Override
            public void process(String streamSegmentName, long targetRate, byte rateType, long startTime, double twoMinuteRate, double fiveMinuteRate, double tenMinuteRate, double twentyMinuteRate) {
                loop.set(false);

                assert twentyMinuteRate > 0;
                assert tenMinuteRate > twentyMinuteRate;
                assert fiveMinuteRate > tenMinuteRate;
                assert twoMinuteRate > fiveMinuteRate;
            }

            @Override
            public void notify(String streamSegmentName, NotificationType type) {
                assert streamSegmentName.equals(STREAM_SEGMENT_NAME);
            }
        });

        SegmentStatsRecorderImpl impl = new SegmentStatsRecorderImpl(monitors, null,
                Duration.ofSeconds(10).toMillis(), Executors.newFixedThreadPool(10), Executors.newSingleThreadScheduledExecutor());
        impl.createSegment(STREAM_SEGMENT_NAME, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 1000);
        impl.record(STREAM_SEGMENT_NAME, 0, 1000);

        long txnCreationTime = System.currentTimeMillis();

        long writeStartTime = System.currentTimeMillis();

        // record data without txn
        while (loop.get()) {
            for (int i = 0; i < 100000; i++) {
                impl.record(STREAM_SEGMENT_NAME, 0, 1000);
            }
            // updating with 100 * 100000 events per second
            Exceptions.handleInterrupted(() -> Thread.sleep(1000));
            if (System.currentTimeMillis() - writeStartTime > Duration.ofSeconds(20).toMillis()) {
                throw new RuntimeException("test went on for over 20 seconds without a record being reported");
            }
        }

        // record data in txn:
        double preTxnRate = impl.getSegmentAggregates(STREAM_SEGMENT_NAME).getTwoMinuteRate();
        impl.merge(STREAM_SEGMENT_NAME, 0, 1000000, txnCreationTime);
        double postTxnRate = impl.getSegmentAggregates(STREAM_SEGMENT_NAME).getTwoMinuteRate();

        assert postTxnRate != preTxnRate;
    }
}
