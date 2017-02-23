/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host.stat;

import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.stream.AckFuture;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Transaction;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;

public class MonitorTest {

    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";

    @Test
    public void monitorTest() {
        EventStreamWriter<ScaleRequest> writer = new EventStreamWriter<ScaleRequest>() {
            @Override
            public AckFuture writeEvent(ScaleRequest event) {
                return null;
            }

            @Override
            public AckFuture writeEvent(String routingKey, ScaleRequest event) {
                assert event != null;

                assert routingKey.equals(String.format("%s/%s", SCOPE, STREAM)) &&
                        event.getScope().equals(SCOPE) &&
                        event.getStream().equals(STREAM) &&
                        event.getDirection() == ScaleRequest.UP;
                return null;
            }

            @Override
            public Transaction<ScaleRequest> beginTxn(long transactionTimeout) {
                return null;
            }

            @Override
            public Transaction<ScaleRequest> getTxn(UUID transactionId) {
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

        AutoScaleProcessor monitor = new AutoScaleProcessor(writer,
                new AutoScalerConfig(Duration.ofMinutes(10), Duration.ofMinutes(10), Duration.ofMinutes(10),
                        Duration.ofMinutes(10), "pravega", "requeststream", null),
                Executors.newFixedThreadPool(10), Executors.newSingleThreadScheduledExecutor());

        String streamSegmentName = Segment.getScopedName(SCOPE, STREAM, 0);
        monitor.notifyCreated(streamSegmentName, WireCommands.CreateSegment.IN_EVENTS_PER_SEC, 10);

        long twentyminutesback = System.currentTimeMillis() - Duration.ofMinutes(20).toMillis();
        monitor.put(streamSegmentName, new ImmutablePair<>(twentyminutesback, twentyminutesback));

        monitor.report(streamSegmentName, 10, WireCommands.CreateSegment.IN_EVENTS_PER_SEC,
                twentyminutesback,
                1001, 500, 200, 200);
    }
}