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

public final class ConnectionSummaryStats {

    private Map<PravegaNodeUri, Connection> minWriterMap = new HashMap<>();
    private Map<PravegaNodeUri, Connection> minReaderMap = new HashMap<>();
    private Map<PravegaNodeUri, Connection> minSessionCountMap = new HashMap<>();

    public void accept(Connection connection) {
        minReaderMap.compute(connection.getUri(), (uri, con) -> con == null ? connection :
                (connection.getReaderCount() < con.getReaderCount()) ? connection : con);
        minWriterMap.compute(connection.getUri(), (uri, con) -> con == null ? connection :
                (connection.getWriterCount() < con.getWriterCount()) ? connection : con);
        minSessionCountMap.compute(connection.getUri(), (uri, con) -> con == null ? connection :
                (connection.getSessionCount() < con.getSessionCount()) ? connection : con);
    }

    // Combiner
    public ConnectionSummaryStats combine(ConnectionSummaryStats other) {
        other.minReaderMap.forEach((uri, con) -> minReaderMap.merge(uri, con, (con1, con2) -> (con1.getReaderCount() < con2.getReaderCount()) ? con1 : con2));
        other.minWriterMap.forEach((uri, con) -> minWriterMap.merge(uri, con, (con1, con2) -> (con1.getWriterCount() < con2.getWriterCount()) ? con1 : con2));
        other.minWriterMap.forEach((uri, con) -> minWriterMap.merge(uri, con, (con1, con2) -> (con1.getSessionCount() < con2.getSessionCount()) ? con1 : con2));
        return this;
    }

    public Optional<Connection> getConnectionWithMinimumWriters(PravegaNodeUri uri) {
        return Optional.ofNullable(minWriterMap.get(uri));
    }

    public Optional<Connection> getConnecionWithMinimumReaders(PravegaNodeUri uri) {
        return Optional.ofNullable(minReaderMap.get(uri));
    }

    public Optional<Connection> getConnectionWithMinimumSession(PravegaNodeUri uri) {
        return Optional.ofNullable(minSessionCountMap.get(uri));
    }
}
