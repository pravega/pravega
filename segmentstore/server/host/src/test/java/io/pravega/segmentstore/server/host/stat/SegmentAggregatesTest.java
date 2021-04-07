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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void aggregateByEvent() {
        setClock(0);
        // test events per second
        TestSegmentAggregatesEvents aggregates = new TestSegmentAggregatesEvents(1);

        // generate a traffic of 5 events per second for 30 minutes
        for (int i = 0; i < 30 * 60; i++) {
            aggregates.update(0, 1);
            aggregates.update(0, 1);
            aggregates.update(0, 1);
            aggregates.update(0, 1);
            aggregates.update(0, 1);
            setClock(i * 1000);
        }

        // the computed rates should be greater than 1 kbps
        assertTrue(aggregates.getTwoMinuteRate() > 4.5 && aggregates.getTwoMinuteRate() < 5.5);
        assertTrue(aggregates.getFiveMinuteRate() > 4.5 && aggregates.getFiveMinuteRate() < 5.5);
        assertTrue(aggregates.getTenMinuteRate() > 4.5 && aggregates.getTenMinuteRate() < 5.5);
        assertTrue(aggregates.getTwentyMinuteRate() > 4.5 && aggregates.getTwentyMinuteRate() < 5.5);
    }

    @Test
    public void aggregateWithSilentPeriods() {
        long time = 0L;
        setClock(time);
        // test events per second
        TestSegmentAggregatesEvents aggregates = new TestSegmentAggregatesEvents(1);
        
        // generate a traffic of 5 events per second for 1 minute
        for (int i = 0; i < 60; i++) {
            aggregates.update(0, 5);
            time += 1000;
            setClock(time);
        }
        // verify that the rate is close to 5 events per second
        assertEquals(5.0, aggregates.getTwoMinuteRate(), 1.0);
        time += 10 * 60 * 1000;
        // advance clock by 10 minutes
        setClock(time);
        aggregates.update(0, 100);

        // the computed rates should be greater than 1 kbps
        assertTrue(aggregates.getTwoMinuteRate() < 1.0);
        
        // verify that sustained traffic resumption gets the rate back up
        for (int i = 0; i < 2 * 60; i++) {
            aggregates.update(0, 5);
            time += 1000;
            setClock(time);
        }
        assertEquals(aggregates.getTwoMinuteRate(), 5.0, 2.0);

    }

    @Test
    public void aggregateByThroughput() {
        setClock(0);
        // test bytes per second
        TestSegmentAggregatesThroughput aggregates = new TestSegmentAggregatesThroughput(1);

        // generate a traffic of 250 * 5 = 1250 bytes per second for 30 minutes
        for (int i = 0; i < 30 * 60; i++) {
            // we are deliberately keeping dataLength less than 1024 for each update
            aggregates.update(250, 0);
            aggregates.update(250, 0);
            aggregates.update(250, 0);
            aggregates.update(250, 0);
            aggregates.update(250, 0);
            setClock(i * 1000);
        }

        // the computed rates should be greater than 1 kbps
        assertTrue(aggregates.getTwoMinuteRate() > 1.0 && aggregates.getTwoMinuteRate() < 1.5);
        assertTrue(aggregates.getFiveMinuteRate() > 1.0 && aggregates.getFiveMinuteRate() < 1.5);
        assertTrue(aggregates.getTenMinuteRate() > 1.0 && aggregates.getTenMinuteRate() < 1.5);
        assertTrue(aggregates.getTwentyMinuteRate() > 1.0 && aggregates.getTwentyMinuteRate() < 1.5);
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
