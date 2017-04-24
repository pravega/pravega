/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl.netty;

import java.util.concurrent.CompletableFuture;

import io.pravega.common.netty.PravegaNodeUri;
import io.pravega.common.netty.ReplyProcessor;

/**
 * A factory that establishes connections to Prevaga servers.
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

}
