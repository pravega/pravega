/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.mocks;

import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.OperationContext;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.TxnStatus;
import com.emc.pravega.controller.store.stream.VersionedTransactionData;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
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
    public CompletableFuture<Pair<VersionedTransactionData, List<Segment>>> createTxn(final String scope, final String stream,
                                                                                      final long lease, final long maxExecutionTime,
                                                                                      final long scaleGracePeriod,
                                                                                      final OperationContext contextOpt) {
        final OperationContext context =
                contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return streamMetadataStore.createTransaction(scope, stream, lease, maxExecutionTime, scaleGracePeriod,
                context, executor)
                .thenCompose(txData -> {
                    log.info("Created transaction {} with version {}", txData.getId(), txData.getVersion());
                    return streamMetadataStore.getActiveSegments(scope, stream, contextOpt, executor)
                            .thenApply(segmentList -> new ImmutablePair<>(txData, segmentList));
                });
    }

    @Override
    public CompletableFuture<TxnStatus> abortTxn(final String scope, final String stream, final UUID txId,
                                                 final Optional<Integer> version,
                                                 final OperationContext contextOpt) {
        final OperationContext context =
                contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return this.streamMetadataStore.sealTransaction(scope, stream, txId, false, version, context, executor)
                .thenApply(status -> {
                    log.info("Sealed:abort transaction {} with version {}", txId, version);
                    return status;
                });
    }

    @Override
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
    public CompletableFuture<TxnStatus> commitTxn(final String scope, final String stream, final UUID txId,
                                                  final OperationContext contextOpt) {
        final OperationContext context =
                contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return this.streamMetadataStore.sealTransaction(scope, stream, txId, true, Optional.<Integer>empty(), context, executor)
                .thenApply(status -> {
                    log.info("Sealed:commit transaction {} with version {}", txId, null);
                    return status;
                });
    }
}

