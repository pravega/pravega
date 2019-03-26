/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;

import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import java.util.concurrent.CompletableFuture;

public interface ConnectionPool {

    CompletableFuture<ClientConnection> getConnection(PravegaNodeUri uri, ReplyProcessor rp);

    void releaseConnection(ClientConnection connection);

}

