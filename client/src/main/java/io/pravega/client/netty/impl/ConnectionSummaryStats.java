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

import io.pravega.shared.protocol.netty.PravegaNodeUri;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This is used to store the stats about the connection.
 */
public final class ConnectionSummaryStats {

    // This need not be threadsafe as because the parallel implementation of Stream.collect() provides the necessary partitioning and
    // isolation for efficient parallel execution.
    private Map<PravegaNodeUri, Connection> minSessionCountMap = new HashMap<>();
    private Map<PravegaNodeUri, Integer> connectionCountMap = new HashMap<>();
    // The below code can be enabled to perform optimization based on Segment Writers count or Segment Reader count.
    // private Map<PravegaNodeUri, Connection> minWriterMap = new HashMap<>();
    // private Map<PravegaNodeUri, Connection> minReaderMap = new HashMap<>();

    // Accumulator.
    public void accept(Connection connection) {
        minSessionCountMap.compute(connection.getUri(), (uri, con) -> con == null ? connection :
                (connection.getSessionCount() < con.getSessionCount()) ? connection : con);
        connectionCountMap.compute(connection.getUri(), (uri, count) -> count == null ? 1 : count + 1);
    }

    // Combiner
    public ConnectionSummaryStats combine(ConnectionSummaryStats other) {
        other.minSessionCountMap.forEach((uri, con) -> minSessionCountMap.merge(uri, con, (con1, con2) -> (con1.getSessionCount() < con2.getSessionCount()) ? con1 : con2));
        other.connectionCountMap.forEach((uri, count) -> connectionCountMap.merge(uri, count, (count1, count2) -> count1 + count2));
        return this;
    }

    public Optional<Connection> getConnectionWithMinimumSession(PravegaNodeUri uri) {
        return Optional.ofNullable(minSessionCountMap.get(uri));
    }

    public int getConnectionCount(PravegaNodeUri uri) {
        return connectionCountMap.getOrDefault(uri, 0);
    }
}
