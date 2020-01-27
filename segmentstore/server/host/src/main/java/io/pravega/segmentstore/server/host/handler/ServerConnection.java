/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
}
