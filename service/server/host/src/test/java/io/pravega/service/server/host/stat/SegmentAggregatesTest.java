/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.service.server.host.stat;

import io.pravega.shared.protocol.netty.WireCommands;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SegmentAggregatesTest {

    @Test
    public void aggregate() {
        SegmentAggregates aggregates = new SegmentAggregates(WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 100);

        write(aggregates, 100);
        aggregates.setLastTick(System.nanoTime() - Duration.ofMillis(5050).toNanos());
        aggregates.update(0, 1);

        assert aggregates.getTwoMinuteRate() > 0 && aggregates.getFiveMinuteRate() > 0 &&
                aggregates.getTenMinuteRate() > 0 && aggregates.getTwentyMinuteRate() > 0;

    }

    @Test
    public void aggregateTxn() {
        SegmentAggregates aggregates = new SegmentAggregates(WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 100);
        aggregates.setLastTick(System.nanoTime() - Duration.ofMillis(5050).toNanos());
        // add transaction. Approximately 10 events per second.
        aggregates.updateTx(0, 6500, System.currentTimeMillis() - Duration.ofMinutes(10).toMillis());
        assert aggregates.getTwoMinuteRate() > 10;

        aggregates = new SegmentAggregates(WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 100);

        aggregates.updateTx(0, 100, System.currentTimeMillis());
        assert aggregates.getTwoMinuteRate() == 0;
        assert aggregates.getCurrentCount().get() == 100;
        aggregates.setLastTick(System.nanoTime() - Duration.ofMillis(5050).toNanos());

        aggregates.updateTx(0, 1000, System.currentTimeMillis() - Duration.ofSeconds(5).toMillis());
        assert aggregates.getTwoMinuteRate() > 0;
    }

    private void write(SegmentAggregates aggregates, int numOfEvents) {
        for (int i = 0; i < numOfEvents; i++) {
            aggregates.update(0, 1);
        }
    }

    @Test
    public void parallel() throws ExecutionException, InterruptedException {
        SegmentAggregates aggregates = new SegmentAggregates(WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 100);
        CompletableFuture.allOf(CompletableFuture.runAsync(() -> write(aggregates, 100)),
                CompletableFuture.runAsync(() -> write(aggregates, 100)),
                CompletableFuture.runAsync(() -> write(aggregates, 100))).get();
        aggregates.setLastTick(System.nanoTime() - Duration.ofMillis(5050).toNanos());
        aggregates.update(0, 1);
        // 301 events in 5 seconds
        assert aggregates.getTwoMinuteRate() > 50;
    }

    private void writeTx(SegmentAggregates aggregates, int numOfEvents) {
        aggregates.updateTx(0, numOfEvents, System.currentTimeMillis() - Duration.ofMillis(10100).toMillis());
    }

    @Test
    public void parallelTx() throws ExecutionException, InterruptedException {
        SegmentAggregates aggregates = new SegmentAggregates(WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 100);
        // effectively write 300 events in 10 seconds
        CompletableFuture.allOf(CompletableFuture.runAsync(() -> writeTx(aggregates, 100)),
                CompletableFuture.runAsync(() -> writeTx(aggregates, 100)),
                CompletableFuture.runAsync(() -> writeTx(aggregates, 100))).get();
        // Two minute rate should be closer to 30 events per sec. So at least > 20 events per second
        aggregates.setLastTick(System.nanoTime() - Duration.ofMillis(5050).toNanos());
        aggregates.update(0, 1);

        assert aggregates.getTwoMinuteRate() > 20;
    }
}
