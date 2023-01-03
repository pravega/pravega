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
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.RequestProcessor;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Data;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class SerializedRequestHandlerTest extends ThreadPooledTestSuite {

    @Test(timeout = 10000)
    public void testProcessEvent() throws InterruptedException, ExecutionException {
        final ConcurrentHashMap<String, List<Integer>> orderOfProcessing = new ConcurrentHashMap<>();

        SerializedRequestHandler<TestEvent> requestHandler = new SerializedRequestHandler<TestEvent>(executorService()) {
            @Override
            public CompletableFuture<Void> processEvent(TestEvent event) {
                orderOfProcessing.compute(event.getKey(), (x, y) -> {
                    if (y == null) {
                        y = new ArrayList<>();
                    }
                    y.add(event.getNumber());
                    return y;
                });
                return event.getFuture();
            }
        };

        List<Pair<TestEvent, CompletableFuture<Void>>> stream1Queue = requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1"));
        assertNull(stream1Queue);
        // post 3 work for stream1
        TestEvent s1e1 = new TestEvent("scope", "stream1", 1);
        CompletableFuture<Void> s1p1 = requestHandler.process(s1e1, () -> false);
        TestEvent s1e2 = new TestEvent("scope", "stream1", 2);
        CompletableFuture<Void> s1p2 = requestHandler.process(s1e2, () -> false);
        TestEvent s1e3 = new TestEvent("scope", "stream1", 3);
        CompletableFuture<Void> s1p3 = requestHandler.process(s1e3, () -> false);

        stream1Queue = requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1"));
        assertTrue(stream1Queue.size() >= 2);
        assertTrue(stream1Queue.stream().noneMatch(x -> x.getRight().isDone()));
        List<Integer> collect = stream1Queue.stream().map(x -> x.getLeft().getNumber()).collect(Collectors.toList());
        assertTrue(collect.indexOf(2) < collect.indexOf(3));

        s1e3.complete();

        stream1Queue = requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1"));

        // verify that no processing is complete
        assertTrue(stream1Queue.size() >= 2);
        assertTrue(stream1Queue.stream().noneMatch(x -> x.getRight().isDone()));
        collect = stream1Queue.stream().map(x -> x.getLeft().getNumber()).collect(Collectors.toList());
        assertTrue(collect.indexOf(2) < collect.indexOf(3));

        // post 3 work for stream2
        TestEvent s2e1 = new TestEvent("scope", "stream2", 1);
        CompletableFuture<Void> s2p1 = requestHandler.process(s2e1, () -> false);
        TestEvent s2e2 = new TestEvent("scope", "stream2", 2);
        CompletableFuture<Void> s2p2 = requestHandler.process(s2e2, () -> false);
        TestEvent s2e3 = new TestEvent("scope", "stream2", 3);
        CompletableFuture<Void> s2p3 = requestHandler.process(s2e3, () -> false);

        List<Pair<TestEvent, CompletableFuture<Void>>> stream2Queue = requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1"));
        assertTrue(stream2Queue.size() >= 2);
        assertTrue(stream2Queue.stream().noneMatch(x -> x.getRight().isDone()));
        collect = stream2Queue.stream().map(x -> x.getLeft().getNumber()).collect(Collectors.toList());
        assertTrue(collect.indexOf(2) < collect.indexOf(3));

        s1e1.complete();
        Futures.await(s1p1);

        stream1Queue = requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1"));
        assertTrue(stream1Queue.size() >= 1);
        assertTrue(stream1Queue.stream().noneMatch(x -> x.getRight().isDone()));
        collect = stream1Queue.stream().map(x -> x.getLeft().getNumber()).collect(Collectors.toList());
        assertTrue(collect.contains(3));

        // now make sure that we have concurrently run for two streams
        s2e1.complete();
        Futures.await(s2p1);

        stream2Queue = requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream2"));
        assertTrue(stream2Queue.size() >= 1);
        assertTrue(stream2Queue.stream().noneMatch(x -> x.getRight().isDone()));
        collect = stream2Queue.stream().map(x -> x.getLeft().getNumber()).collect(Collectors.toList());
        assertTrue(collect.contains(3));

        // now complete all processing
        s2e2.complete();
        Futures.await(s2p2);

        s2e3.complete();

        s1e2.complete();
        Futures.await(s1p2);

        Futures.await(s1p3);
        Futures.await(s2p3);

        assertTrue(orderOfProcessing.get(s1e1.getKey()).get(0) == 1 &&
                orderOfProcessing.get(s1e1.getKey()).get(1) == 2 &&
                orderOfProcessing.get(s1e1.getKey()).get(2) == 3);
        assertTrue(orderOfProcessing.get(s2e1.getKey()).get(0) == 1 &&
                orderOfProcessing.get(s2e1.getKey()).get(1) == 2 &&
                orderOfProcessing.get(s2e1.getKey()).get(2) == 3);

        Futures.loop(() -> requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1")) == null,
                () -> CompletableFuture.completedFuture(null), executorService());
        Futures.loop(() -> requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream2")) == null,
                () -> CompletableFuture.completedFuture(null), executorService());

        // now that we have drained all the work from the processor.
        // let's post new work for stream 1
        TestEvent s1e4 = new TestEvent("scope", "stream1", 4);
        CompletableFuture<Void> s1p4 = requestHandler.process(s1e4, () -> false);

        stream1Queue = requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1"));
        assertNotNull(stream1Queue);

        s1e4.complete();
        Futures.await(s1p4);

        assertTrue(orderOfProcessing.get(s1e1.getKey()).get(3) == 4);
    }

    @Test(timeout = 10000)
    public void testPostponeEvent() throws InterruptedException, ExecutionException {
        AtomicInteger postponeS1e1Count = new AtomicInteger();
        AtomicInteger postponeS1e2Count = new AtomicInteger();
        AtomicBoolean allowCompletion = new AtomicBoolean(false);

        SerializedRequestHandler<TestEvent> requestHandler = new SerializedRequestHandler<TestEvent>(executorService()) {
            @Override
            public CompletableFuture<Void> processEvent(TestEvent event) {
                if (!event.future.isDone()) {
                    return Futures.failedFuture(new TestPostponeException());
                }
                return event.getFuture();
            }

            @Override
            public boolean toPostpone(TestEvent event, long pickupTime, Throwable exception) {

                boolean retval = true;

                if (allowCompletion.get()) {
                    if (event.number == 1) {
                        postponeS1e1Count.incrementAndGet();
                        retval = exception instanceof TestPostponeException && postponeS1e1Count.get() < 2;
                    }

                    if (event.number == 2) {
                        postponeS1e2Count.incrementAndGet();
                        retval = exception instanceof TestPostponeException && (System.currentTimeMillis() - pickupTime < Duration.ofMillis(100).toMillis());
                    }
                }

                return retval;
            }
        };

        List<Pair<TestEvent, CompletableFuture<Void>>> stream1Queue =
                requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1"));
        assertNull(stream1Queue);
        // post 3 work for stream1
        TestEvent s1e1 = new TestEvent("scope", "stream1", 1);
        CompletableFuture<Void> s1p1 = requestHandler.process(s1e1, () -> false);
        TestEvent s1e2 = new TestEvent("scope", "stream1", 2);
        CompletableFuture<Void> s1p2 = requestHandler.process(s1e2, () -> false);
        TestEvent s1e3 = new TestEvent("scope", "stream1", 3);
        CompletableFuture<Void> s1p3 = requestHandler.process(s1e3, () -> false);

        // post events for some more arbitrary streams in background
        AtomicBoolean stop = new AtomicBoolean(false);

        runBackgroundStreamProcessing("stream2", requestHandler, stop);
        runBackgroundStreamProcessing("stream3", requestHandler, stop);
        runBackgroundStreamProcessing("stream4", requestHandler, stop);

        s1e3.complete();
        // verify that s1p3 completes.
        assertTrue(Futures.await(s1p3));
        // verify that s1e1 and s1e2 are still not complete.
        assertTrue(!s1e1.getFuture().isDone());
        assertTrue(!s1p1.isDone());
        assertTrue(!s1e2.getFuture().isDone());
        assertTrue(!s1p2.isDone());

        // Allow completion
        allowCompletion.set(true);

        assertFalse(Futures.await(s1p1));
        assertFalse(Futures.await(s1p2));
        AssertExtensions.assertThrows("", s1p1::join, e -> Exceptions.unwrap(e) instanceof TestPostponeException);
        AssertExtensions.assertThrows("", s1p2::join, e -> Exceptions.unwrap(e) instanceof TestPostponeException);
        assertTrue(postponeS1e1Count.get() == 2);
        assertTrue(postponeS1e2Count.get() > 0);
        stop.set(true);
    }

    @Test(timeout = 10000)
    public void testThrowsSynchronousException() throws InterruptedException, ExecutionException {
        LinkedBlockingQueue<Pair<CompletableFuture<Void>, CompletableFuture<Void>>> signalQueue = new LinkedBlockingQueue<>();
        SerializedRequestHandler<FailingEvent> throwingRequestHandler = new SerializedRequestHandler<FailingEvent>(executorService()) {
            @Override
            public CompletableFuture<Void> processEvent(FailingEvent event) {
                Pair<CompletableFuture<Void>, CompletableFuture<Void>> pair = signalQueue.poll();
                pair.getKey().complete(null);
                pair.getValue().join();
                throw new RuntimeException(event.eventId);
            }
        };
        
        FailingEvent event = new FailingEvent("scope", "stream", "1");
        FailingEvent event2 = new FailingEvent("scope", "stream", "2");
        FailingEvent event3 = new FailingEvent("scope", "stream", "3");
        signalQueue.add(new ImmutablePair<>(new CompletableFuture<>(), CompletableFuture.completedFuture(null)));
        // we should have first event processing throw a synchronous exception
        AssertExtensions.assertFutureThrows("Processing should have failed in procesEvent method with synchronous exception", 
                throwingRequestHandler.process(event, () -> false), 
                e -> Exceptions.unwrap(e) instanceof RuntimeException && Exceptions.unwrap(e).getMessage().equals("1"));

        // verify that the processing is complete and the event is removed from the queue for the stream. 
        List<Pair<FailingEvent, CompletableFuture<Void>>> queue = throwingRequestHandler.getEventQueueForKey(event.getKey());
        assertTrue(queue == null || queue.isEmpty());

        CompletableFuture<Void> wait2 = new CompletableFuture<>();
        CompletableFuture<Void> signal2 = new CompletableFuture<>();
        CompletableFuture<Void> wait3 = new CompletableFuture<>();
        CompletableFuture<Void> signal3 = new CompletableFuture<>();
        signalQueue.add(new ImmutablePair<>(wait2, signal2));
        signalQueue.add(new ImmutablePair<>(wait3, signal3));
        CompletableFuture<Void> future2 = throwingRequestHandler.process(event2, () -> false);
        CompletableFuture<Void> future3 = throwingRequestHandler.process(event3, () -> false);
        
        // processing for 2nd event is called. 
        wait2.join();

        queue = throwingRequestHandler.getEventQueueForKey(event.getKey());
        assertFalse(queue.isEmpty());
        assertEquals(queue.size(), 1);
        assertEquals(queue.get(0).getKey().eventId, "3");

        assertFalse(wait3.isDone());
        // signal 2nd processing to complete
        signal2.complete(null);
        
        // we should be able to process additional events. 
        AssertExtensions.assertFutureThrows("Second processing should have failed in procesEvent method with synchronous exception",
                future2, e -> Exceptions.unwrap(e) instanceof RuntimeException && Exceptions.unwrap(e).getMessage().equals("2"));
        
        signal3.complete(null);
        AssertExtensions.assertFutureThrows("Third processing should have failed in procesEvent method with synchronous exception",
                future3, e -> Exceptions.unwrap(e) instanceof RuntimeException && Exceptions.unwrap(e).getMessage().equals("3"));
    }

    @Test(timeout = 10000)
    public void testCancellation() throws Exception {
        final ConcurrentHashMap<String, List<Integer>> orderOfProcessing = new ConcurrentHashMap<>();

        SerializedRequestHandler<TestEvent> requestHandler = new SerializedRequestHandler<TestEvent>(executorService()) {
            @Override
            public CompletableFuture<Void> processEvent(TestEvent event) {
                orderOfProcessing.compute(event.getKey(), (x, y) -> {
                    if (y == null) {
                        y = new ArrayList<>();
                    }
                    y.add(event.getNumber());
                    return y;
                });
                return event.getFuture();
            }
        };

        String scope = "scope";
        String stream = "stream";
        List<Pair<TestEvent, CompletableFuture<Void>>> queue = requestHandler.getEventQueueForKey(getKeyForStream(scope, stream));
        assertNull(queue);
        AtomicBoolean stop = new AtomicBoolean(false);
        // post 3 work for stream
        TestEvent e1 = new TestEvent(scope, stream, 1);
        CompletableFuture<Void> p1 = requestHandler.process(e1, stop::get);
        TestEvent e2 = new TestEvent(scope, stream, 2);
        CompletableFuture<Void> p2 = requestHandler.process(e2, stop::get);
        TestEvent e3 = new TestEvent(scope, stream, 3);
        CompletableFuture<Void> p3 = requestHandler.process(e3, stop::get);

        queue = requestHandler.getEventQueueForKey(getKeyForStream(scope, stream));
        // ensure that e1 is picked for processing
        AssertExtensions.assertEventuallyEquals(2, () -> requestHandler.getEventQueueForKey(getKeyForStream(scope, stream)).size(),
                10000L);
        assertTrue(queue.stream().noneMatch(x -> x.getRight().isDone()));
        List<Integer> collect = queue.stream().map(x -> x.getLeft().getNumber()).collect(Collectors.toList());
        assertTrue(collect.indexOf(2) < collect.indexOf(3));
        
        // now set stop = true
        stop.set(true);
        
        // verify that until p1 completes nothing else will be processed. 
        queue = requestHandler.getEventQueueForKey(getKeyForStream(scope, stream));
        assertEquals(2, queue.size());
        assertTrue(queue.stream().noneMatch(x -> x.getRight().isDone()));
        
        // now complete processing for event 1. All subsequent events for the stream will be cancelled.
        e1.complete();
        CompletableFuture.allOf(p1, p2, p3)
                         .exceptionally(e -> {
                             if (Exceptions.unwrap(e) instanceof CancellationException) {
                                 return null;
                             } else {
                                 throw new CompletionException(e);
                             }
                         })
                         .join();
        assertTrue(p1.isDone());
        assertTrue(p2.isCancelled());
        assertTrue(p3.isCancelled());
    }

    private void runBackgroundStreamProcessing(String streamName, SerializedRequestHandler<TestEvent> requestHandler, AtomicBoolean stop) {
        CompletableFuture.runAsync(() -> {
            while (!stop.get()) {
                TestEvent event = new TestEvent("scope", streamName, 0);
                event.complete();
                Futures.await(requestHandler.process(event, () -> false));
            }
        });
    }

    private String getKeyForStream(String scope, String stream) {
        return String.format("%s/%s", scope, stream);
    }

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    @Data
    public static class TestEvent implements ControllerEvent {
        private final String scope;
        private final String stream;
        private final int number;
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        @Override
        public String getKey() {
            return String.format("%s/%s", scope, stream);
        }

        @Override
        public CompletableFuture<Void> process(RequestProcessor processor) {
            return CompletableFuture.completedFuture(null);
        }

        public void complete() {
            future.complete(null);
        }
    }

    @Data
    public static class FailingEvent implements ControllerEvent {
        private final String scope;
        private final String stream;
        private final String eventId;
        
        @Override
        public String getKey() {
            return scope + stream;
        }

        @Override
        public CompletableFuture<Void> process(RequestProcessor processor) {
            return CompletableFuture.completedFuture(null);
        }
    }


    private static class TestPostponeException extends RuntimeException {
    }
}
