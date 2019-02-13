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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class SegmentAggregatesTest {
    private final AtomicLong currentTime = new AtomicLong();

    private void setClock(long time) {
        currentTime.set(time);
    }

    @Test
    public void aggregate() {
        setClock(0);
        SegmentAggregates aggregates = new TestSegmentAggregatesEvents(100);

        aggregates.update(0, 100);
        setClock(5001);

        aggregates.update(0, 0);

        assert aggregates.getTwoMinuteRate() > 0 && aggregates.getFiveMinuteRate() > 0 &&
                aggregates.getTenMinuteRate() > 0 && aggregates.getTwentyMinuteRate() > 0;

        setClock(0);
        // test bytes per second
        aggregates = new TestSegmentAggregatesThroughput(1);

        aggregates.update(100000, 0);
        setClock(5001);

        aggregates.update(0, 0);

        assert aggregates.getTwoMinuteRate() > 0 && aggregates.getFiveMinuteRate() > 0 &&
                aggregates.getTenMinuteRate() > 0 && aggregates.getTwentyMinuteRate() > 0;
    }

    @Test
    public void aggregateTxn() {
        setClock(Duration.ofMinutes(10).toMillis() - Duration.ofSeconds(5).toMillis());
        SegmentAggregates aggregates = new TestSegmentAggregatesEvents(100);

        // add transaction. Approximately 10 events per second.
        aggregates.updateTx(0, 6500, 0L);
        setClock(Duration.ofMinutes(10).toMillis() + 1);
        aggregates.update(0, 0);

        assert aggregates.getTwoMinuteRate() > 10;

        setClock(0);
        aggregates = new TestSegmentAggregatesEvents(100);
        aggregates.updateTx(0, 100, 0L);
        assert aggregates.getTwoMinuteRate() == 0;
        assert aggregates.getCurrentCount() == 100;
        setClock(Duration.ofSeconds(5).toMillis() + 1);
        aggregates.updateTx(0, 1000, 0L);
        assert aggregates.getTwoMinuteRate() > 219;
    }

    private void write(SegmentAggregates aggregates, int numOfEvents) {
        aggregates.update(0, numOfEvents);
    }

    @Test
    public void parallel() throws ExecutionException, InterruptedException {
        setClock(0L);
        SegmentAggregates aggregates = new TestSegmentAggregatesEvents(100);
        CompletableFuture.allOf(CompletableFuture.runAsync(() -> write(aggregates, 100)),
                CompletableFuture.runAsync(() -> write(aggregates, 100)),
                CompletableFuture.runAsync(() -> write(aggregates, 100))).get();
        setClock(Duration.ofSeconds(5).toMillis() + 1);
        aggregates.update(0, 0);
        // 300 events in 5.001 seconds
        assert aggregates.getTwoMinuteRate() > 50;
    }

    private void writeTx(SegmentAggregates aggregates, int numOfEvents, long txnCreationTime) {
        aggregates.updateTx(0, numOfEvents, txnCreationTime);
    }

    @Test
    public void parallelTx() throws ExecutionException, InterruptedException {
        setClock(Duration.ofSeconds(10).toMillis());
        SegmentAggregates aggregates = new TestSegmentAggregatesEvents(100);
        CompletableFuture.allOf(CompletableFuture.runAsync(() -> writeTx(aggregates, 100, 0)),
                CompletableFuture.runAsync(() -> writeTx(aggregates, 100, 0)),
                CompletableFuture.runAsync(() -> writeTx(aggregates, 100, 0))).get();

        setClock(Duration.ofSeconds(15).toMillis() + 1);
        aggregates.update(0, 0);

        assert aggregates.getTwoMinuteRate() > 29;
    }

    private class TestSegmentAggregatesEvents extends SegmentAggregates.ByEventCount {
        TestSegmentAggregatesEvents(int targetRate) {
            super(targetRate);
        }

        @Override
        protected long getTimeMillis() {
            return currentTime.get();
        }
    }

    private class TestSegmentAggregatesThroughput extends SegmentAggregates.ByThroughput {
        TestSegmentAggregatesThroughput(int targetRate) {
            super(targetRate);
        }

        @Override
        protected long getTimeMillis() {
            return currentTime.get();
        }
    }
}
