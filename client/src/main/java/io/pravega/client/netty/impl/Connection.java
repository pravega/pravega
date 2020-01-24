/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;

import io.pravega.shared.protocol.netty.PravegaNodeUri;
import java.util.concurrent.CompletableFuture;
import lombok.Data;

/**
 * This class represents a Connection that is established with a Segment Store instance and its
 * attributes. (e.g: FlowCount, WriterCount)
 */
@Data
public class Connection {
    private final PravegaNodeUri uri;
    private final FlowHandler flowHandler;
    
    /**
     * A future that completes when the connection is first established.
     */
    private final CompletableFuture<Void> connected;
    
    /**
     * Returns the number of open flows on this connection.
     * @return Flow count.
     */
    public int getFlowCount() {
        return flowHandler.getOpenFlowCount();
    }
}


