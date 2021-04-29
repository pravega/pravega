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
package io.pravega.client.stream.mock;

import com.google.common.base.Preconditions;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.Flow;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.experimental.Delegate;

import static org.junit.Assert.assertNull;

@RequiredArgsConstructor
public class MockConnectionFactoryImpl implements ConnectionFactory, ConnectionPool {
    Map<PravegaNodeUri, ClientConnection> connections = new HashMap<>();
    Map<PravegaNodeUri, ReplyProcessor> processors = new HashMap<>();
    private ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "testClientInternal");
    private boolean ownsExecutor = true;

    public void setExecutor(ScheduledExecutorService executor) {
        ExecutorServiceHelpers.shutdown(this.executor);
        this.executor = executor;
        this.ownsExecutor = false;
    }

    @Override
    @Synchronized
    public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri location, ReplyProcessor rp) {
        ClientConnection connection = connections.get(location);
        Preconditions.checkState(connection != null, "Unexpected Endpoint");
        ReplyProcessor previous = processors.put(location, rp);
        assertNull("Mock connection factory does not support multiple concurrent connections to the same location", previous);
        return CompletableFuture.completedFuture(new DelegateClientConnection(location, connection));
    }

    @Override
    public ScheduledExecutorService getInternalExecutor() {
        return executor;
    }

    @Synchronized
    public void provideConnection(PravegaNodeUri location, ClientConnection c) {
        connections.put(location, c);
        processors.remove(location);
    }

    @Synchronized
    public ReplyProcessor getProcessor(PravegaNodeUri location) {
        return processors.get(location);
    }

    @Override
    public void close() {
        if (this.ownsExecutor) {
            // Only shut down the executor if it was the one we created. Do not shut down externally-provided executors
            // as that may break any tests that close this factory instance before the test completion.
            ExecutorServiceHelpers.shutdown(executor);
        }
    }

    @Override
    public CompletableFuture<ClientConnection> getClientConnection(Flow flow, PravegaNodeUri uri, ReplyProcessor rp) {
        return establishConnection(uri, rp);
    }

    @Override
    public CompletableFuture<ClientConnection> getClientConnection(PravegaNodeUri uri, ReplyProcessor rp) {
        return establishConnection(uri, rp);
    }

    @Override
    public void getClientConnection(Flow flow, PravegaNodeUri uri, ReplyProcessor rp, CompletableFuture<ClientConnection> connection) {
        establishConnection(uri, rp).thenApply(connection::complete);
    }
    
    @RequiredArgsConstructor
    @EqualsAndHashCode
    private class DelegateClientConnection implements ClientConnection {
        
        private final PravegaNodeUri location;
        
        @Delegate(excludes = AutoCloseable.class)
        private final ClientConnection impl;
        
        @Override
        public void close() {
            processors.remove(location);
            impl.close();
        }
    }
    
}