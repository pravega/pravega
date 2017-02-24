/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host.stat;

import com.emc.pravega.common.netty.WireCommands;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SegmentAggregatesTest {

    @Test
    public void aggregate() {
        SegmentAggregates aggregates = new SegmentAggregates(WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 100);

        write(aggregates);
        assert aggregates.getTwoMinuteRate() > 0 && aggregates.getFiveMinuteRate() > 0 &&
                aggregates.getTenMinuteRate() > 0 && aggregates.getTwentyMinuteRate() > 0;

        // add transaction. Approximately 10 events per second.
        aggregates.updateTx(0, 1100, System.currentTimeMillis() - Duration.ofSeconds(100).toMillis());
        assert aggregates.getTwoMinuteRate() > 10;
    }

    private void write(SegmentAggregates aggregates) {
        long startTime = System.currentTimeMillis();
        // after 10 seconds we should have written ~100 events.
        // Which means 2 minute rate at this point is 100 / 120 ~= 0.4 events per second
        while (System.currentTimeMillis() - startTime < Duration.ofSeconds(10).toMillis()) {
            for (int i = 0; i < 11; i++) {
                aggregates.update(0, 1);
            }
            // Simulating an approximate rate of 10 events per second
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void parallel() throws ExecutionException, InterruptedException {
        SegmentAggregates aggregates = new SegmentAggregates(WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 100);
        CompletableFuture.allOf(CompletableFuture.runAsync(() -> write(aggregates)),
                CompletableFuture.runAsync(() -> write(aggregates)),
                CompletableFuture.runAsync(() -> write(aggregates))).get();
        // 3 writers in parallel would write about 300 events in 10 seconds.
        // 2 minute rate = 300/120 = 2.5.. with exponential weighing it will be slightly less than 2
        // with some approximations because of inaccuracy in measurement, we can potentially get even less.
        // So will check against 1
        assert aggregates.getTwoMinuteRate() > 1.0;
    }
}
