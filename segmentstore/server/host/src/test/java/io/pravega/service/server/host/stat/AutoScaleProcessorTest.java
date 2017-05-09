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

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.AckFuture;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Transaction;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.controller.event.ScaleEvent;
import io.pravega.shared.protocol.netty.WireCommands;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class AutoScaleProcessorTest {

    private static final String SCOPE = "scope";
    private static final String STREAM1 = "stream1";
    private static final String STREAM2 = "stream2";
    private static final String STREAM3 = "stream3";
    ExecutorService executor;
    ScheduledExecutorService maintenanceExecutor;

    @Before
    public void setup() {
        executor = Executors.newSingleThreadExecutor();
        maintenanceExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void teardown() {
        executor.shutdown();
        maintenanceExecutor.shutdown();
    }

    @Test (timeout = 10000)
    public void scaleTest() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture<Void> result2 = new CompletableFuture<>();
        CompletableFuture<Void> result3 = new CompletableFuture<>();
        EventStreamWriter<ScaleEvent> writer = createWriter(event -> {
            if (event.getScope().equals(SCOPE) &&
                    event.getStream().equals(STREAM1) &&
                    event.getDirection() == ScaleEvent.UP) {
                result.complete(null);
            }

            if (event.getScope().equals(SCOPE) &&
                    event.getStream().equals(STREAM2) &&
                    event.getDirection() == ScaleEvent.DOWN) {
                result2.complete(null);
            }

            if (event.getScope().equals(SCOPE) &&
                    event.getStream().equals(STREAM3) &&
                    event.getDirection() == ScaleEvent.DOWN) {
                result3.complete(null);
            }
        });

        AutoScaleProcessor monitor = new AutoScaleProcessor(writer,
                AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                        .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0)
                        .with(AutoScalerConfig.CACHE_CLEANUP_IN_SECONDS, 1)
                        .with(AutoScalerConfig.CACHE_EXPIRY_IN_SECONDS, 1).build(),
                executor, maintenanceExecutor);

        String streamSegmentName1 = Segment.getScopedName(SCOPE, STREAM1, 0);
        String streamSegmentName2 = Segment.getScopedName(SCOPE, STREAM2, 0);
        String streamSegmentName3 = Segment.getScopedName(SCOPE, STREAM3, 0);
        monitor.notifyCreated(streamSegmentName1, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 10);
        monitor.notifyCreated(streamSegmentName2, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 10);
        monitor.notifyCreated(streamSegmentName3, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 10);

        long twentyminutesback = System.currentTimeMillis() - Duration.ofMinutes(20).toMillis();
        monitor.put(streamSegmentName1, new ImmutablePair<>(twentyminutesback, twentyminutesback));
        monitor.put(streamSegmentName3, new ImmutablePair<>(twentyminutesback, twentyminutesback));

        monitor.report(streamSegmentName1, 10, WireCommands.CreateSegment.IN_EVENTS_PER_SEC,
                twentyminutesback,
                1001, 500, 200, 200);

        monitor.report(streamSegmentName3, 10, WireCommands.CreateSegment.IN_EVENTS_PER_SEC,
                twentyminutesback,
                0.0, 0.0, 0.0, 0.0);

        monitor.notifySealed(streamSegmentName1);
        assertTrue(FutureHelpers.await(result));
        assertTrue(FutureHelpers.await(result));
        assertTrue(FutureHelpers.await(result3));
    }

    private EventStreamWriter<ScaleEvent> createWriter(Consumer<ScaleEvent> consumer) {
        return new EventStreamWriter<ScaleEvent>() {
            @Override
            public AckFuture writeEvent(ScaleEvent event) {
                return null;
            }

            @Override
            public AckFuture writeEvent(String routingKey, ScaleEvent event) {
                consumer.accept(event);
                return null;
            }

            @Override
            public Transaction<ScaleEvent> beginTxn(long transactionTimeout, long maxExecutionTime,
                                                    long scaleGracePeriod) {
                return null;
            }

            @Override
            public Transaction<ScaleEvent> getTxn(UUID transactionId) {
                return null;
            }

            @Override
            public EventWriterConfig getConfig() {
                return null;
            }

            @Override
            public void flush() {

            }

            @Override
            public void close() {

            }
        };
    }
}