/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.stream.impl.netty.ClientConnection;
import io.pravega.stream.impl.netty.ConnectionFactory;
import com.google.common.base.Preconditions;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

@RequiredArgsConstructor
public class MockConnectionFactoryImpl implements ConnectionFactory {
    Map<PravegaNodeUri, ClientConnection> connections = new HashMap<>();
    Map<PravegaNodeUri, ReplyProcessor> processors = new HashMap<>();
    final PravegaNodeUri endpoint;

    @Override
    @Synchronized
    public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri location, ReplyProcessor rp) {
        ClientConnection connection = connections.get(location);
        Preconditions.checkState(connection != null, "Unexpected Endpoint");
        processors.put(location, rp);
        return CompletableFuture.completedFuture(connection);
    }

    @Synchronized
    public void provideConnection(PravegaNodeUri location, ClientConnection c) {
        connections.put(location, c);
    }

    @Synchronized
    public ReplyProcessor getProcessor(PravegaNodeUri location) {
        return processors.get(location);
    }

    @Override
    public void close() {
    }
}