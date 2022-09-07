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

import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommand;
import java.util.List;

/**
 * A connection object. Represents the TCP connection in the client process that connects to the
 * server.
 */
public interface ClientConnection extends AutoCloseable {

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
     * Sends the provided append commands.
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

    /**
     * Get the location details of a segment.
     */
    PravegaNodeUri getLocation();

}
