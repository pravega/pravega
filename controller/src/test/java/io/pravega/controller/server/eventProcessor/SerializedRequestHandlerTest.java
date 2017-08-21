/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SerializedRequestHandlerTest extends ThreadPooledTestSuite {

    @Test(timeout = 10000)
    public void testConcurrentEventProcessor() throws InterruptedException, ExecutionException {
        EventProcessor.Writer<TestEvent> writer = event -> CompletableFuture.completedFuture(null);
        final ConcurrentHashMap<String, List<Integer>> orderOfProcessing = new ConcurrentHashMap<>();

        SerializedRequestHandler<TestEvent> requestHandler = new SerializedRequestHandler<TestEvent>(executorService()) {
            @Override
            CompletableFuture<Void> processEvent(TestEvent event, EventProcessor.Writer<TestEvent> writer) {
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
        CompletableFuture<Void> s1p1 = requestHandler.process(s1e1, writer);
        TestEvent s1e2 = new TestEvent("scope", "stream1", 2);
        CompletableFuture<Void> s1p2 = requestHandler.process(s1e2, writer);
        TestEvent s1e3 = new TestEvent("scope", "stream1", 3);
        CompletableFuture<Void> s1p3 = requestHandler.process(s1e3, writer);

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
        CompletableFuture<Void> s2p1 = requestHandler.process(s2e1, writer);
        TestEvent s2e2 = new TestEvent("scope", "stream2", 2);
        CompletableFuture<Void> s2p2 = requestHandler.process(s2e2, writer);
        TestEvent s2e3 = new TestEvent("scope", "stream2", 3);
        CompletableFuture<Void> s2p3 = requestHandler.process(s2e3, writer);

        List<Pair<TestEvent, CompletableFuture<Void>>> stream2Queue = requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1"));
        assertTrue(stream2Queue.size() >= 2);
        assertTrue(stream2Queue.stream().noneMatch(x -> x.getRight().isDone()));
        collect = stream2Queue.stream().map(x -> x.getLeft().getNumber()).collect(Collectors.toList());
        assertTrue(collect.indexOf(2) < collect.indexOf(3));

        s1e1.complete();
        FutureHelpers.await(s1p1);

        stream1Queue = requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1"));
        assertTrue(stream1Queue.size() >= 1);
        assertTrue(stream1Queue.stream().noneMatch(x -> x.getRight().isDone()));
        collect = stream1Queue.stream().map(x -> x.getLeft().getNumber()).collect(Collectors.toList());
        assertTrue(collect.contains(3));

        // now make sure that we have concurrently run for two streams
        s2e1.complete();
        FutureHelpers.await(s2p1);

        stream2Queue = requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1"));
        assertTrue(stream2Queue.size() >= 1);
        assertTrue(stream2Queue.stream().noneMatch(x -> x.getRight().isDone()));
        collect = stream2Queue.stream().map(x -> x.getLeft().getNumber()).collect(Collectors.toList());
        assertTrue(collect.contains(3));

        // now complete all processing
        s2e2.complete();
        FutureHelpers.await(s2p2);

        s2e3.complete();

        s1e2.complete();
        FutureHelpers.await(s1p2);

        FutureHelpers.await(s1p3);
        FutureHelpers.await(s2p3);

        assertTrue(orderOfProcessing.get(s1e1.getKey()).get(0) == 1 &&
                orderOfProcessing.get(s1e1.getKey()).get(1) == 2 &&
                orderOfProcessing.get(s1e1.getKey()).get(2) == 3);
        assertTrue(orderOfProcessing.get(s2e1.getKey()).get(0) == 1 &&
                orderOfProcessing.get(s1e1.getKey()).get(1) == 2 &&
                orderOfProcessing.get(s1e1.getKey()).get(2) == 3);

        FutureHelpers.loop(() -> requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1")) == null,
                () -> CompletableFuture.completedFuture(null), executorService());
        FutureHelpers.loop(() -> requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1")) == null,
                () -> CompletableFuture.completedFuture(null), executorService());

        // now that we have drained all the work from the processor.
        // lets post new work for stream 1
        TestEvent s1e4 = new TestEvent("scope", "stream1", 4);
        CompletableFuture<Void> s1p4 = requestHandler.process(s1e4, writer);

        stream1Queue = requestHandler.getEventQueueForKey(getKeyForStream("scope", "stream1"));
        assertNotNull(stream1Queue);

        s1e4.complete();
        FutureHelpers.await(s1p4);

        assertTrue(orderOfProcessing.get(s1e1.getKey()).get(3) == 4);
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

        public void complete() {
            future.complete(null);
        }
    }
}
