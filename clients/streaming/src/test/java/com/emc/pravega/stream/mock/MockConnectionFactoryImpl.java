/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.stream.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.stream.impl.netty.ClientConnection;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.google.common.base.Preconditions;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

@RequiredArgsConstructor
public class MockConnectionFactoryImpl implements ConnectionFactory {
    Map<PravegaNodeUri, ClientConnection> connections = new HashMap<>();
    Map<PravegaNodeUri, ReplyProcessor> processors = new HashMap<>();
    final PravegaNodeUri endpoint;

    @Override
    @Synchronized
    public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri location, ReplyProcessor rp) {
        ClientConnection connection = connections.get(location);
        Preconditions.checkState(connection != null, "Unexpected Endpoint");
        processors.put(location, rp);
        return CompletableFuture.completedFuture(connection);
    }

    @Synchronized
    public void provideConnection(PravegaNodeUri location, ClientConnection c) {
        connections.put(location, c);
    }

    @Synchronized
    public ReplyProcessor getProcessor(PravegaNodeUri location) {
        return processors.get(location);
    }

    @Override
    public void close() {
    }
}