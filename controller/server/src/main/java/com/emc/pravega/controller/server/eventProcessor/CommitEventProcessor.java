/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessor;
import com.emc.pravega.controller.retryable.RetryableException;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.OperationContext;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * This actor processes commit txn events.
 * It does the following 2 operations in order.
 * 1. Send commit txn message to active segments of the stream.
 * 2. Change txn state from committing to committed.
 */
public class CommitEventProcessor extends EventProcessor<CommitEvent> {

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final ConnectionFactory connectionFactory;
    private final ScheduledExecutorService executor;
    private final SegmentHelper segmentHelper;

    public CommitEventProcessor(final StreamMetadataStore streamMetadataStore,
                                final HostControllerStore hostControllerStore,
                                final ScheduledExecutorService executor,
                                final SegmentHelper segmentHelper,
                                final ConnectionFactory connectionFactory) {
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
        this.connectionFactory = connectionFactory;
    }

    @Override
    protected void process(CommitEvent event, Position position) {
        String scope = event.getScope();
        String stream = event.getStream();
        UUID txId = event.getTxid();
        OperationContext context = streamMetadataStore.createContext(scope, stream);

        streamMetadataStore.getActiveSegments(event.getScope(), event.getStream(), context, executor)
                .thenCompose(segments ->
                        FutureHelpers.allOfWithResults(segments.stream()
                                .parallel()
                                .map(segment -> notifyCommitToHost(scope, stream, segment.getNumber(), txId))
                                .collect(Collectors.toList())))
                .thenCompose(x -> streamMetadataStore.commitTransaction(scope, stream, txId, context, executor));
    }

    private CompletableFuture<Controller.TxnStatus> notifyCommitToHost(final String scope, final String stream, final int segmentNumber, final UUID txId) {
        final long retryInitialDelay = 100;
        final int retryMultiplier = 10;
        final int retryMaxAttempts = 100;
        final long retryMaxDelay = 100000;

        return Retry.withExpBackoff(retryInitialDelay, retryMultiplier, retryMaxAttempts, retryMaxDelay)
                .retryWhen(RetryableException::isRetryable)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> segmentHelper.commitTransaction(scope,
                        stream,
                        segmentNumber,
                        txId,
                        this.hostControllerStore,
                        this.connectionFactory), executor);
    }
}
