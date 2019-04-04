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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.Session;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A Connection factory implementation used to create {@link ClientConnection}s by creating new Session over existing connection pool.
 *
 */
@Slf4j
public final class ConnectionFactoryImpl implements ConnectionFactory {

    private final ClientConfig clientConfig;
    private final ScheduledExecutorService executor;
    @VisibleForTesting
    @Getter
    private final ConnectionPool connectionPool;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ConnectionFactoryImpl(ClientConfig clientConfig) {
        this(clientConfig, new ConnectionPoolImpl(clientConfig), (Integer) null);
    }

    @VisibleForTesting
    public ConnectionFactoryImpl(ClientConfig clientConfig, ConnectionPool connectionPool, Integer numThreadsInPool) {
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        this.connectionPool = connectionPool;
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(getNumThreads(numThreadsInPool), "clientInternal");
    }

    @VisibleForTesting
    public ConnectionFactoryImpl(ClientConfig clientConfig, ConnectionPool connectionPool, ScheduledExecutorService executor) {
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        this.connectionPool = connectionPool;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<ClientConnection> establishConnection(Session session, PravegaNodeUri endpoint, ReplyProcessor rp) {
        return connectionPool.getClientConnection(session, endpoint, rp);
    }

    @Override
    public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri endpoint, ReplyProcessor rp) {
        return connectionPool.getClientConnection(endpoint, rp);
    }

    @Override
    public ScheduledExecutorService getInternalExecutor() {
        return executor;
    }

    @Override
    public void close() {
        log.info("Shutting down connection factory");
        if (closed.compareAndSet(false, true)) {
            ExecutorServiceHelpers.shutdown(executor);
            connectionPool.close();
        }
    }

    @VisibleForTesting
    public int getActiveChannelCount() {
       return connectionPool.getActiveChannelCount();
    }

    private int getNumThreads(Integer numThreadsInPool) {
        if (numThreadsInPool != null) {
            return numThreadsInPool;
        }
        String configuredThreads = System.getProperty("pravega.client.internal.threadpool.size", null);
        if (configuredThreads != null) {
            return Integer.parseInt(configuredThreads);
        }
        return Runtime.getRuntime().availableProcessors();
    }
}
