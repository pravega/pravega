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
package io.pravega.cli.admin.utils;

import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.RawClient;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class AdminSegmentHelper extends SegmentHelper implements AutoCloseable {

    public AdminSegmentHelper(final ConnectionPool connectionPool, HostControllerStore hostStore,
                              ScheduledExecutorService executorService) {
        super(connectionPool, hostStore, executorService);
    }

    public CompletableFuture<WireCommands.StorageFlush> flushToStorage(int containerId, PravegaNodeUri uri, String delegationToken) {
        final WireCommandType type = WireCommandType.FLUSH_TO_STORAGE;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.FlushToStorage request = new WireCommands.FlushToStorage(containerId, delegationToken, requestId);
        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                   handleReply(requestId, r, connection, null, WireCommands.FlushToStorage.class, type);
                   assert r instanceof WireCommands.StorageFlush;
                   return (WireCommands.StorageFlush) r;
                });
    }

    @Override
    public void close() {
        connectionPool.close();
    }
}
