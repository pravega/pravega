/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.requests.ControllerEvent;
import io.pravega.stream.Position;
import io.pravega.stream.impl.PositionInternal;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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
                            case 0:
                                // wait on a latch
                                FutureHelpers.getAndHandleExceptions(latch, RuntimeException::new);
                                if (checkpoint != -1) {
                                    result.completeExceptionally(new RuntimeException("0 not complete yet checkpoint moved ahead"));
                                }
                                break;
                            case 80:
                                // verify the checkpoint
                                if (checkpoint != -1) {
                                    result.completeExceptionally(new RuntimeException("0 not complete yet checkpoint moved ahead"));
                                }

                                // release the latch
                                latch.complete(null);
                                break;
                            default:
                                break;
                        }
                        runningcount.decrementAndGet();
                    }
            );
        }
    }

    ConcurrentEventProcessor<TestEvent, TestRequestHandler> processor;
    int checkpoint = -1;
    CompletableFuture<Void> latch = new CompletableFuture<>();
    CompletableFuture<Void> result = new CompletableFuture<>();

    AtomicInteger runningcount = new AtomicInteger(0);

    @Test(timeout = 10000)
    public void testConcurrentEventProcessor() throws InterruptedException, ExecutionException {
        processor = new ConcurrentEventProcessor<>(new TestRequestHandler(), 2, Executors.newFixedThreadPool(2));
        processor.setCheckpointer(pos -> {
            checkpoint = ((TestPosition) pos).getNumber();

            if (checkpoint < 80) {
                result.completeExceptionally(new RuntimeException("0 not complete yet and checkpoint moved ahead"));
            }
            if (checkpoint == 99) {
                result.complete(null);
            }
        });
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 100; i++) {
                processor.process(new TestEvent(i), new TestPosition(i));
            }
        });
        result.get();
    }
}
