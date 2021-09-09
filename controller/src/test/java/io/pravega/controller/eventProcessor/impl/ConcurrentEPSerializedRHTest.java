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

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.RequestProcessor;
import lombok.Cleanup;
import lombok.Data;
import lombok.Getter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;

public class ConcurrentEPSerializedRHTest {
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    private LinkedBlockingQueue<ControllerEvent> requestStream = new LinkedBlockingQueue<>();
    private List<ControllerEvent> history = Collections.synchronizedList(new ArrayList<>());
    private AtomicReference<String> state = new AtomicReference<>("ACTIVE");
    private EventProcessor.Writer<TestBase> writer = event -> {
        requestStream.add(event);
        history.add(event);
        return CompletableFuture.completedFuture(null);
    };
    private CompletableFuture<Void> waitingForPhase1 = new CompletableFuture<>();
    private CompletableFuture<Long> result1 = new CompletableFuture<>();
    private CompletableFuture<Long> result2 = new CompletableFuture<>();
    private CompletableFuture<Long> result3 = new CompletableFuture<>();

    @Test(timeout = 10000)
    public void testEndToEndRequestProcessingFlow() throws InterruptedException, ExecutionException {
        AtomicBoolean stop = new AtomicBoolean(false);
        TestEvent1 request1 = new TestEvent1("stream", 1);
        TestEvent2 request2 = new TestEvent2("stream", 2);
        TestEvent3 request3 = new TestEvent3("stream", 3);

        // 0. post 3 events in requeststream [e1, e2, e3]
        writer.write(request1).join();
        writer.write(request2).join();
        writer.write(request3).join();

        // process throwing retryable exception. Verify that event is written back and checkpoint has moved forward
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(2, "test");
        TestRequestHandler2 requestHandler = new TestRequestHandler2(executor);
        ConcurrentEventProcessor<TestBase, TestRequestHandler2> processor = new ConcurrentEventProcessor<>(
                requestHandler, 1, executor, null, writer, 1, TimeUnit.SECONDS);

        CompletableFuture.runAsync(() -> {
            while (!stop.get()) {
                ControllerEvent take = Exceptions.handleInterruptedCall(() -> requestStream.take());
                processor.process((TestBase) take, null);
                Exceptions.handleInterrupted(() -> Thread.sleep(100));
            }
        });

        waitingForPhase1.join();

        assertTrue(state.get().equals("STATE1"));

        request1.future.complete(null);

        assertTrue(Futures.await(result1));
        assertTrue(Futures.await(result2));
        assertTrue(Futures.await(result3));
        assertTrue(state.get().equals("ACTIVE"));
        stop.set(true);
    }

    // region exceptions
    private static class TestStartException extends RuntimeException {
    }

    private static class OperationDisallowedException extends RuntimeException {
    }

    private static class RetryableTestException extends RuntimeException implements RetryableException {
    }
    // endregion

    // region test events
    @Data
    private abstract class TestBase implements ControllerEvent {
        private final String stream;
        private final int number;

        @Override
        public String getKey() {
            return stream;
        }
    }

    private class TestEvent1 extends TestBase {
        AtomicInteger retryCount = new AtomicInteger(0);
        CompletableFuture<Void> future = new CompletableFuture<>();

        TestEvent1(String stream, int number) {
            super(stream, number);
        }

        @Override
        public CompletableFuture<Void> process(RequestProcessor processor) {
            state.compareAndSet("ACTIVE", "STATE1");

            // set state to event 1
            if (state.get().equals("STATE1")) {
                // perform workflow
                if (retryCount.getAndIncrement() < 5) {
                    return Futures.failedFuture(new RetryableTestException());
                }
            } else {
                return Futures.failedFuture(new TestStartException());
            }

            waitingForPhase1.complete(null);
            return future.thenAccept(x -> state.compareAndSet("STATE1", "ACTIVE"))
                    .thenAccept(x -> result1.complete(null));
        }
    }

    private class TestEvent2 extends TestBase {
        TestEvent2(String stream, int number) {
            super(stream, number);
        }

        @Override
        public CompletableFuture<Void> process(RequestProcessor processor) {
            state.compareAndSet("ACTIVE", "STATE2");
            if (state.get().equals("STATE2")) {
                return CompletableFuture.completedFuture(null)
                        .thenAccept(x -> state.compareAndSet("STATE2", "ACTIVE"))
                        .thenAccept(x -> result2.complete(null));
            } else {
                return Futures.failedFuture(new OperationDisallowedException());
            }
        }
    }

    private class TestEvent3 extends TestBase {
        TestEvent3(String stream, int number) {
            super(stream, number);
        }

        @Override
        public CompletableFuture<Void> process(RequestProcessor processor) {
            state.compareAndSet("ACTIVE", "STATE3");
            if (state.get().equals("STATE3")) {
                return CompletableFuture.completedFuture(null)
                        .thenAccept(x -> state.compareAndSet("STATE3", "ACTIVE"))
                        .thenAccept(x -> result3.complete(null));
            } else {
                return Futures.failedFuture(new OperationDisallowedException());
            }
        }
    }

    //endregion

    private class TestRequestHandler2 extends SerializedRequestHandler<TestBase> {

        @Getter
        private final List<ControllerEvent> receivedForProcessing;

        TestRequestHandler2(ScheduledExecutorService executor) {
            super(executor);
            receivedForProcessing = Collections.synchronizedList(new ArrayList<>());
        }

        @Override
        public boolean toPostpone(TestBase event, long pickupTime, Throwable exception) {
            return Exceptions.unwrap(exception) instanceof TestStartException;
        }

        @Override
        public CompletableFuture<Void> processEvent(TestBase event) {
            receivedForProcessing.add(event);
            CompletableFuture<Void> result = new CompletableFuture<>();
            Retry.withExpBackoff(100, 1, 5, 100)
                    .retryWhen(RetryableException::isRetryable)
                    .runAsync(() -> event.process(null), executor)
                    .whenCompleteAsync((r, e) -> {
                        if (e != null) {
                            Throwable cause = Exceptions.unwrap(e);
                            if (cause instanceof OperationDisallowedException) {
                                Retry.indefinitelyWithExpBackoff("Error writing event back into requeststream")
                                        .runAsync(() -> writer.write(event), executor)
                                        .thenAccept(v -> result.completeExceptionally(cause));
                            } else {
                                result.completeExceptionally(cause);
                            }
                        } else {
                            result.complete(r);
                        }
                    }, executor);

            return result;
        }
    }
}
