/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.netty;

/**
 * A response going from the server to the client resulting from a previous message from the client to the server.
 */
public interface Reply {
    void process(ReplyProcessor cp);
}
