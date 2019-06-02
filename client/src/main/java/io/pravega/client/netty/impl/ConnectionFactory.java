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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;

/**
 * A factory that establishes connections to Pravega servers.
 * The underlying implementation may or may not implement connection pooling.
 */
public interface ConnectionFactory extends AutoCloseable {

    /**
     * Establishes a connection between server and client with given parameters.
     *
     * @param id       identifier
     * @param endpoint The Pravega Node URI.
     * @param rp       Reply Processor instance.
     * @return An instance of client connection.
     */
    CompletableFuture<ClientConnection> establishConnection(UUID id, PravegaNodeUri endpoint, ReplyProcessor rp);

    /**
     * This method is used to establish a client connection using a {@link Flow} on the underlying Connection
     * pool.
     * @param flow  Flow to be used to create a client connection.
     * @param id     identifier
     * @param endpoint The Pravega Node URI.
     * @param rp Reply Processor instance.
     * @return An instance of client connection.
     */
    CompletableFuture<ClientConnection> establishConnection(Flow flow, UUID id, PravegaNodeUri endpoint, ReplyProcessor rp);

    /**
     * Get the internal executor which is used by the client.
     * @return A ScheduledExecutorService.
     */
    ScheduledExecutorService getInternalExecutor();

    @Override
    void close();

}
