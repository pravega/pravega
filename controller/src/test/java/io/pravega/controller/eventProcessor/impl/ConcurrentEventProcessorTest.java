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
package io.pravega.controller.eventProcessor.impl;

import io.pravega.client.stream.Position;
import io.pravega.client.stream.impl.PositionInternal;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.eventProcessor.RequestHandler;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.RequestProcessor;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        public CompletableFuture<Void> process(TestEvent testEvent, Supplier<Boolean> isCancelled) {
            return Futures.failedFuture(exception);
        }
    }

    private class TestRequestHandler implements RequestHandler<TestEvent> {
        @Override
        public CompletableFuture<Void> process(TestEvent testEvent, Supplier<Boolean> isCancelled) {
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
    private ScheduledExecutorService executor;

    @Before
    public void setup() {
        checkpoint = new AtomicInteger(-1);
        map = new ConcurrentHashMap<>();
        latch = new CompletableFuture<>();
        result = new CompletableFuture<>();
        runningcount = new AtomicInteger(0);
        executor = ExecutorServiceHelpers.newScheduledThreadPool(2, "test");
    }

    @After
    public void tearDown() {
        executor.shutdownNow();    
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
        ConcurrentEventProcessor<TestEvent, TestRequestHandler> processor = new ConcurrentEventProcessor<>(new TestRequestHandler(), 2, executor,
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
                2, executor,
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
                2, executor,
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
                2, executor,
                checkpointer, writer, 1, TimeUnit.SECONDS);

        processor.process(request, new TestPosition(0));

        assertTrue(Futures.await(writerTest));
        assertTrue(!checkpointTest.isDone());
        processor.afterStop();
    }

    @Test(timeout = 10000)
    public void testShutdown() {
        // Submit 3 requests to be processed.
        // Processing of second request should wait on completion of first request. 
        // Issue a shutdown and then complete first and third requests while cancelling second request. 
        CompletableFuture<TestPosition> checkpoint = new CompletableFuture<>();
        TestEvent request0 = new TestEvent(0);
        TestEvent request1 = new TestEvent(1);
        TestEvent request2 = new TestEvent(2);
        EventProcessor.Checkpointer checkpointer = pos -> checkpoint.complete((TestPosition) pos);
        EventProcessor.Writer<TestEvent> writer = event -> CompletableFuture.completedFuture(null);

        CompletableFuture<CompletableFuture<Void>> processing0 = new CompletableFuture<>();
        CompletableFuture<CompletableFuture<Void>> processing1 = new CompletableFuture<>();
        CompletableFuture<CompletableFuture<Void>> processing2 = new CompletableFuture<>();
        
        // process throwing non retryable exception. Verify that no event is written back while the checkpoint has moved forward
        RequestHandler<TestEvent> requestHandler = (event, isCancelled) -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            switch (event.number) {
                case 0:
                    processing0.complete(future);
                    return future;                    
                case 1:
                    processing1.complete(future);
                    // wait until processing 1 completes before beginning processing 2. by that time stop 
                    // should have been invoked as well. 
                    processing0.join().join();
                    if (isCancelled.get()) {
                        future.cancel(true);
                    }
                    return future;
                case 2:
                    processing2.complete(future);
                    return future;
                default:
                    throw new RuntimeException("Unexpected");
            }
        };

        ConcurrentEventProcessor<TestEvent, RequestHandler<TestEvent>> processor = new ConcurrentEventProcessor<>(
                requestHandler, 100, executor,
                checkpointer, writer, 10, TimeUnit.MILLISECONDS);

        processor.process(request0, new TestPosition(0));
        processor.process(request1, new TestPosition(1));
        processor.process(request2, new TestPosition(2));
        
        // send stop asynchronously
        CompletableFuture<Void> stopFuture = CompletableFuture.runAsync(processor::afterStop);
        
        Futures.loop(() -> !processor.isStopFlagSet(), () -> Futures.delayedFuture(Duration.ofMillis(10), executor), executor).join();

        // complete processing of event 1
        processing0.join().complete(null);
        // processing of event 2 should have been cancelled
        AssertExtensions.assertFutureThrows("This should have been cancelled", processing1.join(),
                e -> Exceptions.unwrap(e) instanceof CancellationException);
        // verify that event processor's afterStop has not completed yet.
        assertFalse(stopFuture.isDone());
        // complete processing of event 3 
        processing2.join().complete(null);

        // stop should complete after we have completed all ongoing processing
        stopFuture.join();

        // assert that checkpoint points to event `0`. 
        processor.periodicCheckpoint();
        assertEquals(0, checkpoint.join().number);
    }
}
