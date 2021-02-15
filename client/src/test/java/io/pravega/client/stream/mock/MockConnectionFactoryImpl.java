/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.mock;

import com.google.common.base.Preconditions;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.Flow;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

@RequiredArgsConstructor
public class MockConnectionFactoryImpl implements ConnectionFactory, ConnectionPool {
    Map<PravegaNodeUri, ClientConnection> connections = new HashMap<>();
    Map<PravegaNodeUri, ReplyProcessor> processors = new HashMap<>();
    private ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "testClientInternal");
    private boolean ownsExecutor = true;

    public void setExecutor(ScheduledExecutorService executor) {
        ExecutorServiceHelpers.shutdown(this.executor);
        this.executor = executor;
        this.ownsExecutor = false;
    }

    @Override
    @Synchronized
    public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri location, ReplyProcessor rp) {
        ClientConnection connection = connections.get(location);
        Preconditions.checkState(connection != null, "Unexpected Endpoint");
        processors.put(location, rp);
        return CompletableFuture.completedFuture(connection);
    }

    @Override
    public ScheduledExecutorService getInternalExecutor() {
        return executor;
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
        if (this.ownsExecutor) {
            // Only shut down the executor if it was the one we created. Do not shut down externally-provided executors
            // as that may break any tests that close this factory instance before the test completion.
            ExecutorServiceHelpers.shutdown(executor);
        }
    }

    @Override
    public CompletableFuture<ClientConnection> getClientConnection(Flow flow, PravegaNodeUri uri, ReplyProcessor rp) {
        return establishConnection(uri, rp);
    }

    @Override
    public CompletableFuture<ClientConnection> getClientConnection(PravegaNodeUri uri, ReplyProcessor rp) {
        return establishConnection(uri, rp);
    }
}