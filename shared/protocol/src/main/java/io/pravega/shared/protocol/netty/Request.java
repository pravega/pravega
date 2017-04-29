/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.protocol.netty;

/**
 * A request from the client to the server. Requests usually result in a corresponding Reply being sent back.
 */
public interface Request {
    void process(RequestProcessor cp);
}
