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

import io.pravega.client.Session;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import org.apache.commons.lang3.NotImplementedException;

/**
 * A factory that establishes connections to Pravega servers.
 * The underlying implementation may or may not implement connection pooling.
 */
public interface ConnectionFactory extends AutoCloseable {

    /**
     * Establishes a connection between server and client with given parameters.
     *
     * @param endpoint The Pravega Node URI.
     * @param rp       Reply Processor instance.
     * @return An instance of client connection.
     */
    CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri endpoint, ReplyProcessor rp);

    /**
     * This method is used to establish a client connection using a {@link io.pravega.client.Session} on the underlying Connection pool.
     * @param session  Session id to be used to create a connection.
     * @param endpoint The Pravega Node URI.
     * @param rp Reply Processor instance.
     * @return An instance of client connection.
     */
    default CompletableFuture<ClientConnection> establishConnection(Session session, PravegaNodeUri endpoint, ReplyProcessor rp) {
        throw new NotImplementedException("Connection pooling based client connection has not been implemented");
    }

    /**
     * Get the internal executor which is used by the client.
     * @return A ScheduledExecutorService.
     */
    ScheduledExecutorService getInternalExecutor();

    @Override
    void close();

}
