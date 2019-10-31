/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.eventProcessor.impl;

import io.pravega.client.stream.Position;
import io.pravega.client.stream.impl.PositionInternal;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.eventProcessor.RequestHandler;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.RequestProcessor;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Before;
import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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

        @Override
        public CompletableFuture<Void> process(RequestProcessor processor) {
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

        @Override
        public ByteBuffer toBytes() {
            throw new UnsupportedOperationException();
        }
    }

    private static class RetryableTestException extends RuntimeException implements RetryableException {

    }

    @Data
    private static class TestFailureRequestHandler implements RequestHandler<TestEvent> {
        private final Exception exception;

        @Override
        public CompletableFuture<Void> process(TestEvent testEvent) {
            return Futures.failedFuture(exception);
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
                                Futures.getAndHandleExceptions(latch, RuntimeException::new);
                                if (checkpoint.get() > 2) {
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

    private AtomicInteger checkpoint;
    private ConcurrentHashMap<Integer, Boolean> map;
    private CompletableFuture<Void> latch;
    private CompletableFuture<Void> result;

    private AtomicInteger runningcount;

    @Before
    public void setup() {
        checkpoint = new AtomicInteger(-1);
        map = new ConcurrentHashMap<>();
        latch = new CompletableFuture<>();
        result = new CompletableFuture<>();
        runningcount = new AtomicInteger(0);
    }

    @Test(timeout = 10000)
    public void testConcurrentEventProcessor() throws InterruptedException, ExecutionException {
        EventProcessor.Writer<TestEvent> writer = event -> CompletableFuture.completedFuture(null);

        EventProcessor.Checkpointer checkpointer = pos -> {
            checkpoint.set(((TestPosition) pos).getNumber());

            if (!latch.isDone() && checkpoint.get() > 2) {
                result.completeExceptionally(new IllegalStateException("checkpoint greater than 2"));
            }

            if (checkpoint.get() == 2) {
                if (map.get(0) && map.get(1) && map.get(2) && map.get(4)) {
                    latch.complete(null);
                }
            }

            if (checkpoint.get() == 4) {
                if (map.get(0) && map.get(1) && map.get(2) && map.get(3) && map.get(4)) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(new IllegalStateException("checkpoint 5 while not everything is complete"));
                }
            }
        };
        ConcurrentEventProcessor<TestEvent, TestRequestHandler> processor = new ConcurrentEventProcessor<>(new TestRequestHandler(), 2, Executors.newScheduledThreadPool(2),
                checkpointer, writer, 1, TimeUnit.SECONDS);

        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 5; i++) {
                map.put(i, false);
                processor.process(new TestEvent(i), new TestPosition(i));
            }
        });
        result.get();
        assertTrue(Futures.await(result));
        processor.afterStop();
    }

    @Test(timeout = 10000)
    public void testFailedProcessingRetryable() throws InterruptedException, ExecutionException {
        CompletableFuture<Void> writerTest = new CompletableFuture<>();
        CompletableFuture<Void> checkpointTest = new CompletableFuture<>();
        TestEvent request = new TestEvent(0);
        EventProcessor.Checkpointer checkpointer = pos -> {
            checkpoint.set(((TestPosition) pos).getNumber());
            if (checkpoint.get() == 0) {
                checkpointTest.complete(null);
            } else {
                checkpointTest.completeExceptionally(new RuntimeException());
            }
        };

        EventProcessor.Writer<TestEvent> writer = event -> {
            if (event.equals(request)) {
                writerTest.complete(null);
            } else {
                writerTest.completeExceptionally(new RuntimeException());
            }
            return CompletableFuture.completedFuture(null);
        };

        // process throwing retryable exception. Verify that event is written back and checkpoint has moved forward
        ConcurrentEventProcessor<TestEvent, TestFailureRequestHandler> processor = new ConcurrentEventProcessor<>(new TestFailureRequestHandler(new RetryableTestException()),
                2, Executors.newScheduledThreadPool(2),
                checkpointer, writer, 1, TimeUnit.SECONDS);

        processor.process(request, new TestPosition(0));

        assertTrue(Futures.await(writerTest));
        assertTrue(Futures.await(checkpointTest));
        processor.afterStop();
    }

    @Test(timeout = 10000)
    public void testFailedProcessingNonRetryable() throws InterruptedException, ExecutionException {
        CompletableFuture<Void> writerTest = new CompletableFuture<>();
        CompletableFuture<Void> checkpointTest = new CompletableFuture<>();
        TestEvent request = new TestEvent(0);
        EventProcessor.Checkpointer checkpointer = pos -> {
            checkpoint.set(((TestPosition) pos).getNumber());
            if (checkpoint.get() == 0) {
                checkpointTest.complete(null);
            } else {
                checkpointTest.completeExceptionally(new RuntimeException());
            }
        };

        EventProcessor.Writer<TestEvent> writer = event -> {
            writerTest.complete(null);
            return CompletableFuture.completedFuture(null);
        };

        // process throwing non retryable exception. Verify that no event is written back while the checkpoint has moved forward
        ConcurrentEventProcessor<TestEvent, TestFailureRequestHandler> processor = new ConcurrentEventProcessor<>(
                new TestFailureRequestHandler(new RuntimeException()),
                2, Executors.newScheduledThreadPool(2),
                checkpointer, writer, 1, TimeUnit.SECONDS);

        processor.process(request, new TestPosition(0));

        assertTrue(Futures.await(checkpointTest));
        assertTrue(!writerTest.isDone());
        processor.afterStop();
    }

    @Test(timeout = 10000)
    public void testWriteBackFailed() throws InterruptedException, ExecutionException {
        CompletableFuture<Void> checkpointTest = new CompletableFuture<>();
        CompletableFuture<Void> writerTest = new CompletableFuture<>();
        TestEvent request = new TestEvent(0);
        EventProcessor.Checkpointer checkpointer = pos -> checkpointTest.complete(null);
        AtomicInteger counter = new AtomicInteger();
        EventProcessor.Writer<TestEvent> writer = event -> {
            if (counter.incrementAndGet() > 3) {
                writerTest.complete(null);
                return CompletableFuture.completedFuture(null);
            }
            throw new RetryableTestException();
        };

        // process throwing non retryable exception. Verify that no event is written back while the checkpoint has moved forward
        ConcurrentEventProcessor<TestEvent, TestFailureRequestHandler> processor = new ConcurrentEventProcessor<>(
                new TestFailureRequestHandler(new RetryableTestException()),
                2, Executors.newScheduledThreadPool(2),
                checkpointer, writer, 1, TimeUnit.SECONDS);

        processor.process(request, new TestPosition(0));

        assertTrue(Futures.await(writerTest));
        assertTrue(!checkpointTest.isDone());
        processor.afterStop();
    }

}
