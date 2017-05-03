/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.mocks;

import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.client.stream.impl.netty.ConnectionFactory;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Mock StreamTransactionMetadataTasks class.
 */
@Slf4j
public class MockStreamTransactionMetadataTasks extends StreamTransactionMetadataTasks {

    private final StreamMetadataStore streamMetadataStore;

    public MockStreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                              final HostControllerStore hostControllerStore,
                                              final TaskMetadataStore taskMetadataStore,
                                              final SegmentHelper segmentHelper,
                                              final ScheduledExecutorService executor,
                                              final String hostId,
                                              final ConnectionFactory connectionFactory) {
        super(streamMetadataStore, hostControllerStore, taskMetadataStore, segmentHelper, executor, hostId, connectionFactory);
        this.streamMetadataStore = streamMetadataStore;
    }

    @Override
    @Synchronized
    public CompletableFuture<Pair<VersionedTransactionData, List<Segment>>> createTxn(final String scope, final String stream,
                                                                                      final long lease, final long maxExecutionTime,
                                                                                      final long scaleGracePeriod,
                                                                                      final OperationContext contextOpt) {
        final OperationContext context =
                contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
        final UUID txnId = UUID.randomUUID();
        return streamMetadataStore.createTransaction(scope, stream, txnId, lease, maxExecutionTime, scaleGracePeriod,
                context, executor)
                .thenCompose(txData -> {
                    log.info("Created transaction {} with version {}", txData.getId(), txData.getVersion());
                    return streamMetadataStore.getActiveSegments(scope, stream, context, executor)
                            .thenApply(segmentList -> new ImmutablePair<>(txData, segmentList));
                });
    }

    @Override
    @Synchronized
    public CompletableFuture<TxnStatus> abortTxn(final String scope, final String stream, final UUID txId,
                                                 final Integer version,
                                                 final OperationContext contextOpt) {
        final OperationContext context =
                contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return this.streamMetadataStore.sealTransaction(scope, stream, txId, false, Optional.ofNullable(version), context, executor)
                .thenApply(status -> {
                    log.info("Sealed:abort transaction {} with version {}", txId, version);
                    return status;
                })
                .thenCompose(x -> streamMetadataStore.abortTransaction(scope, stream, txId, context, executor));
    }

    @Override
    @Synchronized
    public CompletableFuture<VersionedTransactionData> pingTxn(final String scope, final String stream,
                                                               final UUID txId, final long lease,
                                                               final OperationContext contextOpt) {
        final OperationContext context =
                contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return streamMetadataStore.pingTransaction(scope, stream, txId, lease, context, executor)
                .thenApply(txData -> {
                    log.info("Pinged transaction {} with version {}", txId, txData.getVersion());
                    return txData;
                });
    }

    @Override
    @Synchronized
    public CompletableFuture<TxnStatus> commitTxn(final String scope, final String stream, final UUID txId,
                                                  final OperationContext contextOpt) {
        final OperationContext context =
                contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return this.streamMetadataStore.sealTransaction(scope, stream, txId, true, Optional.<Integer>empty(), context, executor)
                .thenApply(status -> {
                    log.info("Sealed:commit transaction {} with version {}", txId, null);
                    return status;
                })
                .thenCompose(ignore -> streamMetadataStore.commitTransaction(scope, stream, txId, context, executor));
    }
}

