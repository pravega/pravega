/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl.netty;

import java.util.concurrent.Future;

import io.pravega.common.netty.Append;
import io.pravega.common.netty.ConnectionFailedException;
import io.pravega.common.netty.WireCommand;

/**
 * A connection object. Represents the TCP connection in the client process that connects to the server.
 */
public interface ClientConnection extends AutoCloseable {

    /**
     * Sends a wire command asynchronously.
     *
     * @param cmd The wire comment to be sent.
     * @return A future.
     */
    Future<Void> sendAsync(WireCommand cmd);

    /**
     * Sends the provided command. This operation may block. (Though buffering is used to try to
     * prevent it)
     *
     * @param cmd The command to send.
     * @throws ConnectionFailedException The connection has died, and can no longer be used.
     */
    void send(WireCommand cmd) throws ConnectionFailedException;

    /**
     * Sends the provided append request. This operation may block.
     * (Though buffering is used to try to prevent it)
     *
     * @param append The append command to send.
     * @throws ConnectionFailedException The connection has died, and can no longer be used.
     */
    void send(Append append) throws ConnectionFailedException;

    /**
     * Drop the connection. No further operations may be performed.
     */
    @Override
    void close();
}
