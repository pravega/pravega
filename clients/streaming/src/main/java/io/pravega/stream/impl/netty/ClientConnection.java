/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl.netty;

import io.pravega.common.netty.Append;
import io.pravega.common.netty.ConnectionFailedException;
import io.pravega.common.netty.WireCommand;
import java.util.List;

/**
 * A connection object. Represents the TCP connection in the client process that connects to the
 * server.
 */
public interface ClientConnection extends AutoCloseable {

    /**
     * Sends a wire command asynchronously.
     *
     * @param cmd The wire comment to be sent.
     */
    void sendAsync(WireCommand cmd);

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
     * Sends the provided append requests.
     *
     * @param appends A list of append command to send.
     * @param callback A callback to be invoked when the operation is complete
     */
    void sendAsync(List<Append> appends, CompletedCallback callback);

    /**
     * Drop the connection. No further operations may be performed.
     */
    @Override
    void close();

    @FunctionalInterface
    interface CompletedCallback {
        /**
         * Invoked when the {@link ClientConnection#sendAsync(List, CompletedCallback)} data has
         * either been written to the wire or failed.
         * 
         * @param e The exception that was encountered (Or null if it is a success)
         */
        void complete(ConnectionFailedException e);
    }

}
