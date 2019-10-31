/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;

import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import java.util.concurrent.CompletableFuture;

/**
 * This represents a ConnectionPool that manages the actual network connections to different SegmentStore instances.
 */
public interface ConnectionPool extends AutoCloseable {

    /**
     * This is used to create a {@link ClientConnection} on an existing Connection pool. The Connection pool implementation
     * decides if a new connection needs to be established to the PravegaNode or an existing connection can be reused to establish
     * the connection.
     * @param flow Flow
     * @param uri The Pravega Node Uri.
     * @param rp ReplyProcessor instance.
     * @return An instance of client connection.
     */
    CompletableFuture<ClientConnection> getClientConnection(Flow flow, PravegaNodeUri uri, ReplyProcessor rp);

    /**
     * This is used to create a {@link ClientConnection} where flows are disabled. This implies that only one ClientConnection
     * can exist on the underlying connection.
     *
     * @param uri The Pravega Node Uri.
     * @param rp ReplyProcessor instance.
     * @return An instance of client connection.
     */
    CompletableFuture<ClientConnection> getClientConnection(PravegaNodeUri uri, ReplyProcessor rp);

    /**
     * Fetch the current active {@link io.netty.channel.Channel} count, which represents the number of active connections being
     * managed by the connection pool.
     * @return the number of active Channel.
     */
    int getActiveChannelCount();

    @Override
    void close();
}

