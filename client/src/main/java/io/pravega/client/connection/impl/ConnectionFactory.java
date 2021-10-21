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

    @Override
    void close();

    /**
     * Returns the client-internal thread pool for background tasks.
     */
    ScheduledExecutorService getInternalExecutor();

}
