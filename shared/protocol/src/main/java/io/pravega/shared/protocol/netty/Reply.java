/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

/**
 * A response going from the server to the client resulting from a previous message from the client to the server.
 */
public interface Reply {
    long getRequestId();

    void process(ReplyProcessor cp);

    default boolean isFailure() {
        return false;
    }
}
