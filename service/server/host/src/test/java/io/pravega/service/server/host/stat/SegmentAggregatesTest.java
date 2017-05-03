/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

        write(aggregates);
        assert aggregates.getTwoMinuteRate() > 0 && aggregates.getFiveMinuteRate() > 0 &&
                aggregates.getTenMinuteRate() > 0 && aggregates.getTwentyMinuteRate() > 0;

    }

    @Test
    public void aggregateTxn() {
        SegmentAggregates aggregates = new SegmentAggregates(WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 100);

        // add transaction. Approximately 10 events per second.
        aggregates.updateTx(0, 6500, System.currentTimeMillis() - Duration.ofMinutes(10).toMillis());
        assert aggregates.getTwoMinuteRate() > 10;

        aggregates = new SegmentAggregates(WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 100);

        aggregates.updateTx(0, 100, System.currentTimeMillis());
        assert aggregates.getTwoMinuteRate() == 0;
        assert aggregates.getCurrentCount().get() == 100;

        aggregates.updateTx(0, 1000, System.currentTimeMillis() - Duration.ofSeconds(5).toMillis());
        assert aggregates.getTwoMinuteRate() > 0;
    }

    private void write(SegmentAggregates aggregates) {
        long startTime = System.currentTimeMillis();
        // after 10 seconds we should have written ~100 events.
        // Which means 2 minute rate at this point is 100 / 120 ~= 0.4 events per second
        while (System.currentTimeMillis() - startTime < Duration.ofSeconds(7).toMillis()) {
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
        // 3 writers in parallel would write about 150 events in 5 seconds.
        // 2 minute rate = 150/120 = 1.25.. with exponential weighing it will be slightly less than that.
        // safer side we will check against 0.5
        assert aggregates.getTwoMinuteRate() > 0.5;
    }
}
