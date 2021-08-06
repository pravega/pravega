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

/**
 * Used by the Admin CLI for interacting with the admin-gateway on the Segment Store.
 */
public class AdminSegmentHelper extends SegmentHelper implements AutoCloseable {

    public AdminSegmentHelper(final ConnectionPool connectionPool, HostControllerStore hostStore,
                              ScheduledExecutorService executorService) {
        super(connectionPool, hostStore, executorService);
    }

    /**
     * This method sends a WireCommand to flush the container corresponding to the given containerId to storage.
     *
     * @param containerId     The Id of the container that needs to be persisted to storage.
     * @param uri             The uri of the Segment Store instance.
     * @param delegationToken The token to be presented to the Segment Store.
     * @return A CompletableFuture that will complete normally when the provided keys are deleted.
     * If the operation failed, the future will be failed with the causing exception. If the exception can be
     * retried then the future will be failed.
     */
    public CompletableFuture<WireCommands.StorageFlushed> flushToStorage(int containerId, PravegaNodeUri uri, String delegationToken) {
        final WireCommandType type = WireCommandType.FLUSH_TO_STORAGE;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.FlushToStorage request = new WireCommands.FlushToStorage(containerId, delegationToken, requestId);
        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                   handleReply(requestId, r, connection, null, WireCommands.FlushToStorage.class, type);
                   assert r instanceof WireCommands.StorageFlushed;
                   return (WireCommands.StorageFlushed) r;
                });
    }
}
