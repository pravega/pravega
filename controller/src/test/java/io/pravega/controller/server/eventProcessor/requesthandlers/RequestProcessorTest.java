/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.RequestProcessor;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Data;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class RequestProcessorTest extends ThreadPooledTestSuite {

    @Override
    public int getThreadPoolSize() {
        return 2;
    }

    @Data
    public static class TestEvent1 implements ControllerEvent {
        private final String scope;
        private final String stream;
        private final Supplier<CompletableFuture<Void>> toExecute;

        @Override
        public String getKey() {
            return scope + stream;
        }

        @Override
        public CompletableFuture<Void> process(RequestProcessor processor) {
            return ((TestRequestProcessor1) processor).testProcess(this);
        }
    }

    @Data
    public static class TestEvent2 implements ControllerEvent {
        private final String scope;
        private final String stream;
        private final Supplier<CompletableFuture<Void>> toExecute;

        @Override
        public String getKey() {
            return scope + stream;
        }

        @Override
        public CompletableFuture<Void> process(RequestProcessor processor) {
            return ((TestRequestProcessor2) processor).testProcess(this);
        }
    }

    public static class TestRequestProcessor1 extends AbstractRequestProcessor<TestEvent1> implements StreamTask<TestEvent1> {
        private final BlockingQueue<TestEvent1> queue;
        private boolean toIgnoreFairness;
        public TestRequestProcessor1(StreamMetadataStore streamMetadataStore, ScheduledExecutorService executor, BlockingQueue<TestEvent1> queue) {
            super(streamMetadataStore, executor);
            this.queue = queue;
            this.toIgnoreFairness = false;
        }

        public CompletableFuture<Void> testProcess(TestEvent1 event) {
            return withCompletion(this, event, event.scope, event.stream, OPERATION_NOT_ALLOWED_PREDICATE);
        }

        @Override
        public CompletableFuture<Void> execute(TestEvent1 event) {
            return event.toExecute.get();
        }

        @Override
        public CompletableFuture<Void> writeBack(TestEvent1 event) {
            queue.add(event);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Boolean> hasTaskStarted(TestEvent1 event) {
            return CompletableFuture.completedFuture(toIgnoreFairness);
        }
    }

    public static class TestRequestProcessor2 extends AbstractRequestProcessor<TestEvent2> implements StreamTask<TestEvent2> {
        private final BlockingQueue<TestEvent2> queue;

        public TestRequestProcessor2(StreamMetadataStore streamMetadataStore, ScheduledExecutorService executor, BlockingQueue<TestEvent2> queue) {
            super(streamMetadataStore, executor);
            this.queue = queue;
        }

        public CompletableFuture<Void> testProcess(TestEvent2 event) {
            return withCompletion(this, event, event.scope, event.stream, OPERATION_NOT_ALLOWED_PREDICATE);
        }

        @Override
        public CompletableFuture<Void> execute(TestEvent2 event) {
            return event.toExecute.get();
        }

        @Override
        public CompletableFuture<Void> writeBack(TestEvent2 event) {
            queue.add(event);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Boolean> hasTaskStarted(TestEvent2 event) {
            return CompletableFuture.completedFuture(false);
        }
    }

    abstract StreamMetadataStore getStore();

    @Test(timeout = 30000)
    public void testRequestProcessor() throws InterruptedException {
        BlockingQueue<TestEvent1> queue1 = new LinkedBlockingQueue<>();
        TestRequestProcessor1 requestProcessor1 = new TestRequestProcessor1(getStore(), executorService(), queue1);

        BlockingQueue<TestEvent2> queue2 = new LinkedBlockingQueue<>();
        TestRequestProcessor2 requestProcessor2 = new TestRequestProcessor2(getStore(), executorService(), queue2);

        String stream = "test";
        String scope = "test";
        CompletableFuture<Void> started1 = new CompletableFuture<>();
        CompletableFuture<Void> started2 = new CompletableFuture<>();
        CompletableFuture<Void> waitForIt1 = new CompletableFuture<>();
        CompletableFuture<Void> waitForIt2 = new CompletableFuture<>();

        TestEvent1 event11 = new TestEvent1(scope, stream, () -> {
            started1.complete(null);
            waitForIt1.join();
            return CompletableFuture.completedFuture(null);
        });
        TestEvent1 event12 = new TestEvent1(scope, stream, () -> CompletableFuture.completedFuture(null));

        TestEvent2 event21 = new TestEvent2(scope, stream, () -> Futures.failedFuture(StoreException.create(StoreException.Type.OPERATION_NOT_ALLOWED, "Failing processing")));
        TestEvent2 event22 = new TestEvent2(scope, stream, () -> {
            started2.complete(null);
            waitForIt2.join();
            return CompletableFuture.completedFuture(null);
        });

        // 1. start test event1 processing on processor 1. Don't let this complete.
        CompletableFuture<Void> processing11 = requestProcessor1.process(event11);
        // wait to ensure it is started.
        started1.join();

        // 2. start test event2 processing on processor 2. Make this fail with OperationNotAllowed and verify that it gets postponed.
        AssertExtensions.assertFutureThrows("Fail first processing with operation not allowed", requestProcessor2.process(event21),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        // also verify that store has set the processor name of processor 2.
        String waitingProcessor = getStore().getWaitingRequestProcessor(scope, stream, null, executorService()).join();
        assertEquals(TestRequestProcessor2.class.getSimpleName(), waitingProcessor);
        TestEvent2 taken2 = requestProcessor2.queue.take();
        assertEquals(taken2, event21);

        // 3. signal processing on processor 1 to complete.
        waitForIt1.complete(null);

        // processing11 should complete successfully.
        processing11.join();

        // 4. submit another processing for processor1. this should get postponed too but processor name should not change.
        AssertExtensions.assertFutureThrows("This should fail and event should be reposted", requestProcessor1.process(event12),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        TestEvent1 taken1 = requestProcessor1.queue.take();
        assertEquals(taken1, event12);

        // 5. now try processing event on processor 2. this should start successfully.
        CompletableFuture<Void> processing22 = requestProcessor2.process(event22);
        started2.join();
        // 6. try to start a new processing on processor 1 while processing on `2` is ongoing. This should fail but should not be able
        // to change the processor name.
        AssertExtensions.assertFutureThrows("This should fail without even starting", requestProcessor1.process(event12),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        waitingProcessor = getStore().getWaitingRequestProcessor(scope, stream, null, executorService()).join();
        assertEquals(TestRequestProcessor2.class.getSimpleName(), waitingProcessor);
        taken1 = requestProcessor1.queue.take();
        assertEquals(taken1, event12);

        // 7. complete processing on `2`.
        waitForIt2.complete(null);
        processing22.join();

        // 8. verify that wait processor name is cleaned up.
        waitingProcessor = getStore().getWaitingRequestProcessor(scope, stream, null, executorService()).join();
        assertEquals(null, waitingProcessor);
    }

    @Test(timeout = 30000)
    public void testIgnoreFairness() throws InterruptedException {
        BlockingQueue<TestEvent1> queue1 = new LinkedBlockingQueue<>();
        TestRequestProcessor1 requestProcessor1 = new TestRequestProcessor1(getStore(), executorService(), queue1);

        BlockingQueue<TestEvent2> queue2 = new LinkedBlockingQueue<>();
        TestRequestProcessor2 requestProcessor2 = new TestRequestProcessor2(getStore(), executorService(), queue2);

        String stream = "test";
        String scope = "test";
        CompletableFuture<Void> started1 = new CompletableFuture<>();
        CompletableFuture<Void> waitForIt1 = new CompletableFuture<>();

        TestEvent1 event1 = new TestEvent1(scope, stream, () -> {
            started1.complete(null);
            return waitForIt1;
        });

        TestEvent2 event2 = new TestEvent2(scope, stream, () -> Futures.failedFuture(StoreException.create(StoreException.Type.OPERATION_NOT_ALLOWED, "Failing processing")));

        // 1. start test event1 processing on processor 1. Don't let this complete.
        CompletableFuture<Void> processing11 = requestProcessor1.process(event1);
        // wait to ensure it is started.
        started1.join();

        // 2. start test event2 processing on processor 2. Make this fail with OperationNotAllowed and verify that it gets postponed.
        AssertExtensions.assertFutureThrows("Fail first processing with operation not allowed", requestProcessor2.process(event2),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        // also verify that store has set the processor name of processor 2.
        String waitingProcessor = getStore().getWaitingRequestProcessor(scope, stream, null, executorService()).join();
        assertEquals(TestRequestProcessor2.class.getSimpleName(), waitingProcessor);
        TestEvent2 taken2 = requestProcessor2.queue.take();
        assertEquals(taken2, event2);

        // 3. Fail processing on processor 1 
        waitForIt1.completeExceptionally(new RuntimeException());

        // processing11 should complete successfully.
        AssertExtensions.assertFutureThrows("", processing11, e -> Exceptions.unwrap(e) instanceof RuntimeException); 

        // set ignore fairness to true
        requestProcessor1.toIgnoreFairness = true;
        // 4. re submit processing for processor1. this should get be picked while we ignore fairness.
        event1 = new TestEvent1(scope, stream, () -> CompletableFuture.completedFuture(null));

        requestProcessor1.process(event1).join();
        assertTrue(requestProcessor1.queue.isEmpty());

        // 5. verify that wait processor name is still set to processor 2
        waitingProcessor = getStore().getWaitingRequestProcessor(scope, stream, null, executorService()).join();
        assertEquals(TestRequestProcessor2.class.getSimpleName(), waitingProcessor);

        // 6. now set ignore fairness to false. The processing of event 1 should be disallowed because of fairness
        requestProcessor1.toIgnoreFairness = false;
        // we should get operation not allowed exception
        AssertExtensions.assertFutureThrows("", requestProcessor1.process(event1), 
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        // event should be posted back
        assertEquals(requestProcessor1.queue.take(), event1);

        // waiting processor should not change.
        waitingProcessor = getStore().getWaitingRequestProcessor(scope, stream, null, executorService()).join();
        assertEquals(TestRequestProcessor2.class.getSimpleName(), waitingProcessor);
    }
}
