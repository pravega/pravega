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
package io.pravega.segmentstore.server.host.handler;

import io.pravega.shared.protocol.netty.RequestProcessor;
import io.pravega.shared.protocol.netty.WireCommand;

/**
 * A connection object. Represents the TCP connection in the server process that is coming from the client.
 */
public interface ServerConnection extends AutoCloseable {

    /**
     * Sends the provided command asynchronously. This operation is non-blocking.
     *
     * @param cmd The command to send.
     */
    void send(WireCommand cmd);

    /**
     * Sets the command processor to receive incoming commands from the client. This
     * method may only be called once.
     *
     * @param cp The Request Processor to set.
     */
    void setRequestProcessor(RequestProcessor cp);

    void pauseReading();

    void resumeReading();

    /**
     * Drop the connection. No further operations may be performed.
     */
    @Override
    void close();

    /**
     * Checks if this instance is already closed.
     *
     * @return {@code true} if this object is already closed, otherwise returns {@code false}.
     */
    boolean isClosed();
}
