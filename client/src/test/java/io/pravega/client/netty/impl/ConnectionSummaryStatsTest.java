/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;


import com.google.common.collect.ImmutableList;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class ConnectionSummaryStatsTest {

    private SessionHandler sessionHandler = mock(SessionHandler.class);
    private CompletableFuture<SessionHandler> sessionHandlerFuture = CompletableFuture.completedFuture(sessionHandler);

    @Test
    public void connectionSummaryStatsTest() {

        PravegaNodeUri nodeUri = new PravegaNodeUri("endpoint1", 1);
        List<Connection> connectionList = ImmutableList.of(getConnection(nodeUri, 4),
                                                           getConnection(nodeUri, 2),
                                                           getConnection(nodeUri, 3),
                                                           getConnection(nodeUri, 1),
                                                           getConnection(nodeUri, 0));
        final Collector<Connection, ConnectionSummaryStats, ConnectionSummaryStats> collectorStats =
                Collector.of(ConnectionSummaryStats::new, ConnectionSummaryStats::accept, ConnectionSummaryStats::combine,
                             Collector.Characteristics.IDENTITY_FINISH);
        ConnectionSummaryStats r = connectionList.parallelStream().collect(collectorStats);
        assertEquals(getConnection(nodeUri, 0), r.getConnectionWithMinimumSession(new PravegaNodeUri("endpoint1", 1)).get());
        assertEquals(5, r.getConnectionCount(new PravegaNodeUri("endpoint1", 1)));
    }

    @Test
    public void connectionStatsWithMultipleEndpointTest() {

        PravegaNodeUri nodeUri1 = new PravegaNodeUri("endpoint1", 1);
        PravegaNodeUri nodeUri2 = new PravegaNodeUri("endpoint1", 2);
        List<Connection> connectionList = ImmutableList.of(getConnection(nodeUri1, 4),
                                                           getConnection(nodeUri2, 2),
                                                           getConnection(nodeUri1, 3),
                                                           getConnection(nodeUri2, 1),
                                                           getConnection(nodeUri1, 0));
        final Collector<Connection, ConnectionSummaryStats, ConnectionSummaryStats> collectorStats =
                Collector.of(ConnectionSummaryStats::new, ConnectionSummaryStats::accept, ConnectionSummaryStats::combine,
                             Collector.Characteristics.IDENTITY_FINISH);
        ConnectionSummaryStats r = connectionList.parallelStream().collect(collectorStats);
        assertEquals(getConnection(nodeUri1, 0), r.getConnectionWithMinimumSession(new PravegaNodeUri("endpoint1", 1)).get());
        assertEquals(getConnection(nodeUri2, 1), r.getConnectionWithMinimumSession(new PravegaNodeUri("endpoint1", 2)).get());
        assertEquals(3, r.getConnectionCount(nodeUri1));
        assertEquals(2, r.getConnectionCount(nodeUri2));
    }


    private Connection getConnection(PravegaNodeUri nodeUri, int sessionCount) {
        return new Connection(nodeUri, sessionHandlerFuture, sessionCount);
    }

}