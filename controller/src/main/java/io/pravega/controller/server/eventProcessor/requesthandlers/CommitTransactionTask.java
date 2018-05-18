/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.google.common.base.Preconditions;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.CommitEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Request handler for processing commit events in request-stream.
 */
@Slf4j
public class CommitTransactionTask implements StreamTask<CommitEvent> {
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;
    private final BlockingQueue<CommitEvent> processedEvents;

    @VisibleForTesting
    public CommitTransactionTask(final StreamMetadataStore streamMetadataStore,
                                final StreamMetadataTasks streamMetadataTasks,
                                final ScheduledExecutorService executor,
                                final BlockingQueue<CommitEvent> queue) {
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.executor = executor;
        this.processedEvents = queue;
    }

    public CommitTransactionTask(final StreamMetadataTasks streamMetadataTasks,
                                 final StreamMetadataStore streamMetadataStore,
                                 final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetadataStore);
        Preconditions.checkNotNull(streamMetadataTasks);
        Preconditions.checkNotNull(executor);
        this.streamMetadataTasks = streamMetadataTasks;
        this.streamMetadataStore = streamMetadataStore;
        this.executor = executor;
        this.processedEvents = null;
    }

    @Override
    public CompletableFuture<Void> execute(CommitEvent event) {
        // TODO: shivesh
        // what do we do for commit..
        // 1. create the "new" what-to-commit znode in metadata store
        // 2. if node already exists and doesnt match event.transactionId, throw operation not allowed. dont worry,
        // it will be posted back in the stream and retried later. Generally if a transaction commit starts, it will come to
        // an end.. but during failover, 
        // once we have created the node, we are guaranteed that it will be only that transaction that will be getting
        // committed at that time.. so post failover even when we recover, we will handle one transaction at a time.
        String scope = event.getScope();
        String stream = event.getStream();
        int epoch = event.getEpoch();
        UUID txnId = event.getTxid();
        OperationContext context = streamMetadataStore.createContext(scope, stream);
        log.debug("Committing transaction {} on stream {}/{}", event.getTxid(), event.getScope(), event.getStream());

        return streamMetadataStore.getActiveEpoch(scope, stream, context, false, executor).thenComposeAsync(pair -> {
            // Note, transaction's epoch either equals stream's current epoch or is one more than it,
            // because stream scale operation ensures that all transactions in current epoch are
            // complete before transitioning the stream to new epoch.
            if (epoch < pair.getKey()) {
                return CompletableFuture.completedFuture(null);
            } else if (epoch == pair.getKey()) {
                // If the transaction's epoch is same as the stream's current epoch, commit it.
                return completeCommit(scope, stream, epoch, txnId, context, this.streamMetadataTasks.retrieveDelegationToken());
            } else {
                throw StoreException.create(StoreException.Type.OPERATION_NOT_ALLOWED,
                        "Transaction on old epoch should complete before allowing commits on new epoch.");
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
        }, executor);
    }

    @Override
    public CompletableFuture<Void> writeBack(CommitEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }

    private CompletableFuture<Void> completeCommit(final String scope,
                                                   final String stream,
                                                   final int epoch,
                                                   final UUID txnId,
                                                   final OperationContext context, String delegationToken) {
        return streamMetadataStore.getActiveSegmentIds(scope, stream, epoch, context, executor)
                .thenComposeAsync(segments -> streamMetadataTasks.notifyTxnCommit(scope, stream, segments, txnId).thenComposeAsync(x ->
                        streamMetadataStore.commitTransaction(scope, stream, epoch, txnId, context, executor), executor)
                        .thenApply(x -> null))
                .thenCompose(x -> Futures.toVoid(streamMetadataTasks.tryCompleteScale(scope, stream, epoch, context, delegationToken)));
    }
}
