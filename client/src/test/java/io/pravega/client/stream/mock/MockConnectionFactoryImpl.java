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
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.Flow;
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
public class MockConnectionFactoryImpl implements ConnectionFactory {
    Map<PravegaNodeUri, ClientConnection> connections = new HashMap<>();
    Map<PravegaNodeUri, ReplyProcessor> processors = new HashMap<>();

    private ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "testClientInternal");
    private ClientConfig clientConfig = ClientConfig.builder().enableTesting(true).build();
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
    @Synchronized
    public CompletableFuture<ClientConnection> establishConnection(Flow flow, PravegaNodeUri location, ReplyProcessor rp) {
      return establishConnection(location, rp);
    }

    @Override
    public ScheduledExecutorService getInternalExecutor() {
        return executor;
    }

    @Override
    public ClientConfig getClientConfig() {
        return clientConfig;
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
}