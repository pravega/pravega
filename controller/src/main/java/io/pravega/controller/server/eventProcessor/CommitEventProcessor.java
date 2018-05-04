/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.Position;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.WriteFailedException;
import io.pravega.shared.controller.event.CommitEvent;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.segment.StreamSegmentNameUtils.getPrimaryId;

/**
 * This actor processes commit txn events.
 * It does the following 2 operations in order.
 * 1. Send commit txn message to active segments of the stream.
 * 2. Change txn state from committing to committed.
 */
@Slf4j
public class CommitEventProcessor extends EventProcessor<CommitEvent> {

    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final HostControllerStore hostControllerStore;
    private final ConnectionFactory connectionFactory;
    private final ScheduledExecutorService executor;
    private final SegmentHelper segmentHelper;
    private final BlockingQueue<CommitEvent> processedEvents;

    @VisibleForTesting
    public CommitEventProcessor(final StreamMetadataStore streamMetadataStore,
                                final StreamMetadataTasks streamMetadataTasks,
                                final HostControllerStore hostControllerStore,
                                final ScheduledExecutorService executor,
                                final SegmentHelper segmentHelper,
                                final ConnectionFactory connectionFactory,
                                final BlockingQueue<CommitEvent> queue) {
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
        this.connectionFactory = connectionFactory;
        this.processedEvents = queue;
    }

    public CommitEventProcessor(final StreamMetadataStore streamMetadataStore,
                                final StreamMetadataTasks streamMetadataTasks,
                                final HostControllerStore hostControllerStore,
                                final ScheduledExecutorService executor,
                                final SegmentHelper segmentHelper,
                                final ConnectionFactory connectionFactory) {
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
        this.connectionFactory = connectionFactory;
        this.processedEvents = null;
    }

    @Override
    protected void process(CommitEvent event, Position position) {
        String scope = event.getScope();
        String stream = event.getStream();
        int epoch = event.getEpoch();
        UUID txnId = event.getTxid();
        OperationContext context = streamMetadataStore.createContext(scope, stream);
        log.debug("Committing transaction {} on stream {}/{}", event.getTxid(), event.getScope(), event.getStream());

        streamMetadataStore.getActiveEpoch(scope, stream, context, false, executor).thenComposeAsync(pair -> {
            // Note, transaction's epoch either equals stream's current epoch or is one more than it,
            // because stream scale operation ensures that all transactions in current epoch are
            // complete before transitioning the stream to new epoch.
            if (epoch < pair.getKey()) {
                return CompletableFuture.completedFuture(null);
            } else if (epoch == pair.getKey()) {
                // If the transaction's epoch is same as the stream's current epoch, commit it.
                return completeCommit(scope, stream, epoch, txnId, context, this.streamMetadataTasks.retrieveDelegationToken());
            } else {
                // Otherwise, postpone commit operation until the stream transitions to next epoch.
                return postponeCommitEvent(event);
            }
        }).whenCompleteAsync((result, error) -> {
            if (error != null) {
                log.error("Failed committing transaction {} on stream {}/{}", txnId, scope, stream);
            } else {
                log.debug("Successfully committed transaction {} on stream {}/{}", txnId, scope, stream);
                if (processedEvents != null) {
                    processedEvents.offer(event);
                }
            }
        }, executor).join();
    }

    private CompletableFuture<Void> completeCommit(final String scope,
                                                   final String stream,
                                                   final int epoch,
                                                   final UUID txnId,
                                                   final OperationContext context, String delegationToken) {
        return streamMetadataStore.getActiveSegmentIds(scope, stream, epoch, context, executor)
                .thenComposeAsync(segments -> notifyCommitToHost(scope, stream, segments, txnId, delegationToken).thenComposeAsync(x ->
                        streamMetadataStore.commitTransaction(scope, stream, epoch, txnId, context, executor), executor)
                                                                                                                 .thenApply(x -> null))
                .thenCompose(x -> Futures.toVoid(streamMetadataTasks.tryCompleteScale(scope, stream, epoch, context, delegationToken)));
    }

    private CompletableFuture<Void> postponeCommitEvent(CommitEvent event) {
        return Retry.indefinitelyWithExpBackoff("Error writing event back into CommitStream")
                .runAsync(() -> writeEvent(event), executor);
    }

    private CompletableFuture<Void> writeEvent(CommitEvent event) {
        UUID txnId = event.getTxid();
        log.debug("Transaction {}, pushing back CommitEvent to commitStream", txnId);
        return this.getSelfWriter().write(event).handleAsync((v, e) -> {
            if (e == null) {
                log.debug("Transaction {}, sent request to commitStream", txnId);
                return null;
            } else {
                Throwable realException = Exceptions.unwrap(e);
                log.warn("Transaction {}, failed sending event to commitStream. Exception: {} Retrying...", txnId,
                        realException.getClass().getSimpleName());
                throw new WriteFailedException(realException);
            }
        }, executor);
    }

    private CompletableFuture<Void> notifyCommitToHost(final String scope, final String stream,
                                                       final List<Long> segments, final UUID txnId, String delegationToken) {
        return Futures.allOf(segments.stream()
                                     .parallel()
                                     .map(segment -> notifyCommitToHost(scope, stream, segment, txnId, delegationToken))
                                     .collect(Collectors.toList()));
    }

    private CompletableFuture<Controller.TxnStatus> notifyCommitToHost(final String scope, final String stream,
                                                                       final long segment, final UUID txId, String delegationToken) {
        String failureMessage = String.format("Transaction = %s, error sending commit notification for segment %d",
                txId, segment);
        return Retry.indefinitelyWithExpBackoff(failureMessage).runAsync(() -> segmentHelper.commitTransaction(scope,
                stream, getPrimaryId(segment), txId, this.hostControllerStore, this.connectionFactory, delegationToken), executor);
    }
}
