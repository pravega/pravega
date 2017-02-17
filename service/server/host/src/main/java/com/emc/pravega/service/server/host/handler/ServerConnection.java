/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.host.handler;

import com.emc.pravega.common.netty.RequestProcessor;
import com.emc.pravega.common.netty.WireCommand;

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
