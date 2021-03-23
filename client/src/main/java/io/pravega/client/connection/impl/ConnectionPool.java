/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.connection.impl;

import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

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
     * This is used to create a {@link ClientConnection} on an existing Connection pool. The Connection pool implementation
     * decides if a new connection needs to be established to the PravegaNode or an existing connection can be reused to establish
     * the connection.
     * @param flow Flow
     * @param uri The Pravega Node Uri.
     * @param rp ReplyProcessor instance.
     * @param connection instance of client connection.
     */
    void getClientConnection(Flow flow, PravegaNodeUri uri, ReplyProcessor rp, CompletableFuture<ClientConnection> connection);

    @Override
    void close();

    /**
     * Returns the client internal thread pool executor.
     */
    ScheduledExecutorService getInternalExecutor();
}

