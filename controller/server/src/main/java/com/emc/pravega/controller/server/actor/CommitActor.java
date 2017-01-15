/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.server.actor;

import com.emc.pravega.common.concurrent.FutureCollectionHelper;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.actor.impl.Actor;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.server.rpc.v1.WireCommandFailedException;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.TransactionStatus;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import lombok.AllArgsConstructor;
import lombok.Data;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * This actor processes commit txn events.
 * It does the following 2 operations in order.
 * 1. Send commit txn message to active segments of the stream.
 * 2. Change txn state from committing to committed.
 */
public class CommitActor extends Actor {

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final ConnectionFactory connectionFactory;
    private final ScheduledExecutorService executor;

    public CommitActor(final StreamMetadataStore streamMetadataStore, final HostControllerStore hostControllerStore) {
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.connectionFactory = new ConnectionFactoryImpl(false);
        this.executor = Executors.newScheduledThreadPool(5);
    }

    @Override
    protected void receive(final byte[] event) throws Exception {
        CommitEvent commitEvent = new CommitEvent(event);
        String scope = commitEvent.scope;
        String stream = commitEvent.stream;
        UUID txId = commitEvent.txid;

        streamMetadataStore.getActiveSegments(commitEvent.getStream())
                .thenCompose(segments ->
                        FutureCollectionHelper.sequence(segments.stream()
                                .parallel()
                                .map(segment -> notifyCommitToHost(scope, stream, segment.getNumber(), txId))
                                .collect(Collectors.toList())))
                .thenCompose(x -> streamMetadataStore.commitTransaction(scope, stream, txId));
    }

    private CompletableFuture<TransactionStatus> notifyCommitToHost(final String scope, final String stream, final int segmentNumber, final UUID txId) {
        final long retryInitialDelay = 100;
        final int retryMultiplier = 10;
        final int retryMaxAttempts = 100;
        final long retryMaxDelay = 100000;

        return Retry.withExpBackoff(retryInitialDelay, retryMultiplier, retryMaxAttempts, retryMaxDelay)
                .retryingOn(WireCommandFailedException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> SegmentHelper.commitTransaction(scope,
                        stream,
                        segmentNumber,
                        txId,
                        this.hostControllerStore,
                        this.connectionFactory), executor);
    }

    @Data
    @AllArgsConstructor
    public static class CommitEvent {
        private final String scope;
        private final String stream;
        private final UUID txid;

        public CommitEvent(final byte[] bytes) {
            throw new NotImplementedException();
        }

        public byte[] getBytes() {
            throw new NotImplementedException();
        }
    }

}
