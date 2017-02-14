/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.stream.impl.netty;

import java.util.concurrent.Future;

import com.emc.pravega.common.netty.Append;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.WireCommand;

/**
 * A connection object. Represents the TCP connection in the client process that connects to the server.
 */
public interface ClientConnection extends AutoCloseable {

    /**
     * Sends a wire command asynchronously.
     *
     * @param cmd The wire comment to be sent.
     * @return A future.
     */
    Future<Void> sendAsync(WireCommand cmd);

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
     * Drop the connection. No further operations may be performed.
     */
    @Override
    void close();
}
