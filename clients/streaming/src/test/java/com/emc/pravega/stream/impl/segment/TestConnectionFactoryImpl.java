package com.emc.pravega.stream.impl.segment;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.stream.impl.StreamController;
import com.google.common.base.Preconditions;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

@RequiredArgsConstructor
class TestConnectionFactoryImpl implements ConnectionFactory, StreamController {
    Map<String, ClientConnection> connections = new HashMap<>();
    Map<String, ReplyProcessor> processors = new HashMap<>();
    final String endpoint;
    
    @Override
    @Synchronized
    public CompletableFuture<ClientConnection> establishConnection(String endpoint, ReplyProcessor rp) {
        ClientConnection connection = connections.get(endpoint);
        Preconditions.checkState(connection != null, "Unexpected Endpoint");
        processors.put(endpoint, rp);
        return CompletableFuture.completedFuture(connection);
    }

    @Synchronized
    void provideConnection(String endpoint, ClientConnection c) {
        connections.put(endpoint, c);
    }

    @Synchronized
    ReplyProcessor getProcessor(String endpoint) {
        return processors.get(endpoint);
    }

    @Override
    public void close() {
    }

    @Override
    public String getEndpointForSegment(String segment) {
        return endpoint;
    }
}