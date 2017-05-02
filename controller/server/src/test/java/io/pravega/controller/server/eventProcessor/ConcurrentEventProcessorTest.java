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
package io.pravega.controller.server.eventProcessor;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.stream.Position;
import io.pravega.stream.impl.PositionInternal;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class ConcurrentEventProcessorTest {

    @Data
    @AllArgsConstructor
    private static class TestEvent implements ControllerEvent {
        int number;

        @Override
        public String getKey() {
            return null;
        }
    }

    @Data
    @AllArgsConstructor
    private static class TestPosition implements Position {
        int number;

        @Override
        public PositionInternal asImpl() {
            return null;
        }
    }

    private class TestRequestHandler implements RequestHandler<TestEvent> {
        @Override
        public CompletableFuture<Void> process(TestEvent testEvent) {
            if (runningcount.getAndIncrement() > 2) {
                result.completeExceptionally(new RuntimeException("max concurrent not honoured"));
            }
            return CompletableFuture.runAsync(() -> {
                        switch (testEvent.getNumber()) {
                            case 3:
                                FutureHelpers.getAndHandleExceptions(latch, RuntimeException::new);
                                if (checkpoint > 2) {
                                    result.completeExceptionally(new RuntimeException("3 still running yet checkpoint moved ahead"));
                                }
                                break;
                            default:
                                break;
                        }
                        map.put(testEvent.getNumber(), true);
                        runningcount.decrementAndGet();
                    }
            );
        }
    }

    ConcurrentEventProcessor<TestEvent, TestRequestHandler> processor;
    int checkpoint = -1;
    Map<Integer, Boolean> map = new HashMap<>();
    CompletableFuture<Void> latch = new CompletableFuture<>();
    CompletableFuture<Void> result = new CompletableFuture<>();

    AtomicInteger runningcount = new AtomicInteger(0);

    @Test(timeout = 100000)
    public void testConcurrentEventProcessor() throws InterruptedException, ExecutionException {
        processor = new ConcurrentEventProcessor<>(new TestRequestHandler(), 2, Executors.newScheduledThreadPool(2),
                pos -> {
                    checkpoint = ((TestPosition) pos).getNumber();

                    if (!latch.isDone() && checkpoint > 2) {
                        result.completeExceptionally(new IllegalStateException("checkpoint greater than 2"));
                    }

                    if (checkpoint == 2) {
                        if (map.get(0) && map.get(1) && map.get(2) && map.get(4)) {
                            latch.complete(null);
                        }
                    }

                    if (checkpoint == 4) {
                        if (map.get(0) && map.get(1) && map.get(2) && map.get(3) && map.get(4)) {
                            result.complete(null);
                        } else {
                            result.completeExceptionally(new IllegalStateException("checkpoint 5 while not everything is complete"));
                        }
                    }
                }, 1, TimeUnit.SECONDS);

        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 5; i++) {
                map.put(i, false);
                processor.process(new TestEvent(i), new TestPosition(i));
            }
        });
        result.get();
        assertTrue(FutureHelpers.await(result));
        processor.afterStop();
    }
}
