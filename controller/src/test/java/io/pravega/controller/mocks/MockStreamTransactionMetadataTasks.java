/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.mocks;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.Version;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus.Status;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Mock StreamTransactionMetadataTasks class.
 */
@Slf4j
public class MockStreamTransactionMetadataTasks extends StreamTransactionMetadataTasks {

    private final StreamMetadataStore streamMetadataStore;

    public MockStreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                              final HostControllerStore hostControllerStore,
                                              final SegmentHelper segmentHelper,
                                              final ScheduledExecutorService executor,
                                              final String hostId,
                                              final ConnectionFactory connectionFactory,
                                              boolean authEnabled,
                                              String tokenSigningKey) {
        super(streamMetadataStore, hostControllerStore, segmentHelper, executor, hostId, connectionFactory, new AuthHelper(authEnabled, tokenSigningKey));
        this.streamMetadataStore = streamMetadataStore;
    }

    @Override
    @Synchronized
    public CompletableFuture<Pair<VersionedTransactionData, List<Segment>>> createTxn(final String scope, final String stream,
                                                                                      final long lease,
                                                                                      final OperationContext contextOpt) {
        final OperationContext context =
                contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
        final UUID txnId = streamMetadataStore.generateTransactionId(scope, stream, null, executor).join();
        return streamMetadataStore.createTransaction(scope, stream, txnId, lease, 10 * lease,
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
                                                 final Version version,
                                                 final OperationContext contextOpt) {
        final OperationContext context =
                contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return this.streamMetadataStore.sealTransaction(scope, stream, txId, false, Optional.ofNullable(version), context, executor)
                .thenApply(pair -> {
                    log.info("Sealed:abort transaction {} with version {}", txId, version);
                    return pair;
                })
                .thenCompose(x -> streamMetadataStore.abortTransaction(scope, stream, txId, context, executor));
    }

    @Override
    @Synchronized
    public CompletableFuture<PingTxnStatus> pingTxn(final String scope, final String stream,
                                                               final UUID txId, final long lease,
                                                               final OperationContext contextOpt) {
        final OperationContext context =
                contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return streamMetadataStore.getTransactionData(scope, stream, txId, context, executor).thenComposeAsync(data ->
                streamMetadataStore.pingTransaction(scope, stream, data, lease, context, executor)
                        .thenApply(txData -> {
                            log.info("Pinged transaction {} with version {}", txId, txData.getVersion());
                            return PingTxnStatus.newBuilder().setStatus(Status.OK).build();
                        }));
    }

    @Override
    @Synchronized
    public CompletableFuture<TxnStatus> commitTxn(final String scope, final String stream, final UUID txId,
                                                  final OperationContext contextOpt) {
        final OperationContext context =
                contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return this.streamMetadataStore.sealTransaction(scope, stream, txId, true, Optional.empty(), context, executor)
                .thenApply(pair -> {
                    log.info("Sealed:commit transaction {} with version {}", txId, null);
                    return pair;
                })
                .thenCompose(x -> streamMetadataStore.commitTransaction(scope, stream, txId, context, executor));
    }
}

