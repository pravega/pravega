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
import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.CommitEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Request handler for processing commit events in request-stream.
 */
@Slf4j
public class CommitTransactionTask implements StreamTask<CommitEvent> {
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;
    private final BlockingQueue<CommitEvent> processedEvents;

    public CommitTransactionTask(final StreamMetadataStore streamMetadataStore,
                                 final StreamMetadataTasks streamMetadataTasks,
                                 final ScheduledExecutorService executor) {
        this(streamMetadataStore, streamMetadataTasks, executor, null);
    }

    @VisibleForTesting
    public CommitTransactionTask(final StreamMetadataStore streamMetadataStore,
                                 final StreamMetadataTasks streamMetadataTasks,
                                 final ScheduledExecutorService executor,
                                 final BlockingQueue<CommitEvent> queue) {
        Preconditions.checkNotNull(streamMetadataStore);
        Preconditions.checkNotNull(streamMetadataTasks);
        Preconditions.checkNotNull(executor);
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.executor = executor;
        this.processedEvents = queue;
    }

    @Override
    public CompletableFuture<Void> execute(CommitEvent event) {
        // 1. get epoch.. check if the epoch matches the epoch in commit event.. if true, we can proceed.. else throw OperationNotAllowedException
        // 2. check if txn-commit-node exists.
        //      if node exists and has same epoch, process the node.
        // 3. if node doesnt exist, collect all committing txn in the epoch and create txn-commit-node.
        // 4. loop over the transactions and commit one transaction at a time until all such transactions are committed.
        // 5. delete txn-commit-node
        // 6. try complete scale.
        // Note: Once scale is completed, all events for this epoch will become a no-op as all transactions would have been committed already.
        String scope = event.getScope();
        String stream = event.getStream();
        int epoch = event.getEpoch();
        OperationContext context = streamMetadataStore.createContext(scope, stream);
        log.debug("Attempting to commit available transactions on epoch {} on stream {}/{}", event.getEpoch(), event.getScope(), event.getStream());

        return streamMetadataStore.getActiveEpoch(scope, stream, context, false, executor).thenCompose(pair -> {
            if (epoch < pair.getKey()) {
                log.debug("Epoch {} on stream {}/{} is already complete.", epoch, scope, stream);
                return CompletableFuture.completedFuture(null);
            } else if (epoch == pair.getKey()) {
                // If the transaction's epoch is same as the stream's current epoch, commit it.
                return tryCommitTransactions(scope, stream, epoch, context, this.streamMetadataTasks.retrieveDelegationToken());
            } else {
                // If commit request if from higher epoch, throw OperationNotAllowed. This will result in event being posted back.
                String message = String.format("Transaction on old epoch should complete before allowing commits on new epoch %d on stream %s/%s",
                        epoch, scope, stream);
                throw StoreException.create(StoreException.Type.OPERATION_NOT_ALLOWED, message);
            }
        }).whenCompleteAsync((result, error) -> {
            if (error != null) {
                log.error("Exception while attempting to committ transaction on epoch {} on stream {}/{}", epoch, scope, stream, error);
            } else {
                log.debug("Successfully committed transactions on epoch {} on stream {}/{}", epoch, scope, stream);
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

    private CompletableFuture<Void> tryCommitTransactions(final String scope,
                                                  final String stream,
                                                  final int epoch,
                                                  final OperationContext context,
                                                  final String delegationToken) {
        // if node already exists and doesnt match event.epoch, throw operation not allowed. dont worry,
        // it will be posted back in the stream and retried later. Generally if a transaction commit starts, it will come to
        // an end.. but during failover,
        // once we have created the node, we are guaranteed that the order of commit will be as per the list.
        CompletableFuture<List<UUID>> txnListFuture = createAndGetTxnCommitRecord(scope, stream, epoch, context);

        CompletableFuture<Void> commitFuture = txnListFuture
                .thenCompose(transactionsToCommit -> streamMetadataStore.getActiveSegmentIds(scope, stream, epoch, context, executor)
                .thenCompose(segments -> {
                    // Chain all transaction commit futures one after the other. This will ensure that order of commit
                    // if honoured and is based on the order in the list.
                    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
                    for (UUID txnId : transactionsToCommit) {
                        log.debug("Committing transaction {} on stream {}/{}", txnId, scope, stream);
                        future = future.thenCompose(v -> streamMetadataTasks.notifyTxnCommit(scope, stream, segments, txnId)
                                .thenCompose(x -> streamMetadataStore.commitTransaction(scope, stream, epoch, txnId, context, executor)
                                        .thenAccept(done -> {
                                            log.debug("transaction {} on stream {}/{} committed successfully", txnId, scope, stream);
                                        })));
                    }

                    return future;
                }));

        // once all commits are done, delete commit txn node
        return commitFuture.thenCompose(v -> streamMetadataStore.deleteTxnCommitList(scope, stream, context, executor)
                .thenCompose(x -> Futures.toVoid(streamMetadataTasks.tryCompleteScale(scope, stream, epoch, context, delegationToken))));
    }

    private CompletableFuture<List<UUID>> createAndGetTxnCommitRecord(String scope, String stream, int epoch, OperationContext context) {
        return streamMetadataStore.getCommittingTransactionsRecord(scope, stream, context, executor)
                .thenCompose(record -> {
                    if (record == null) {
                        // no ongoing commits on transactions.
                        return createNewTxnCommitList(scope, stream, epoch, context, executor);
                    } else {
                        // check if the epoch in record matches current epoch. if not throw OperationNotAllowed
                        if (record.getEpoch() == epoch) {
                            // Note: If there are transactions that are not included in the commitList but have committing state,
                            // we can be sure they will be completed through another event for this epoch.
                            return CompletableFuture.completedFuture(record.getTransactionsToCommit());
                        } else {
                            log.debug("Postponing commit on epoch {} as transactions on different epoch {} are being committed for stream {}/{}",
                                    epoch, record.getEpoch(), scope, stream);
                            throw StoreException.create(StoreException.Type.OPERATION_NOT_ALLOWED,
                                    "Transactions on different epoch are being committed");
                        }
                    }
                });
    }

    private CompletableFuture<List<UUID>> createNewTxnCommitList(String scope, String stream, int epoch,
                                                                 OperationContext context, ScheduledExecutorService executor) {
        return streamMetadataStore.getTransactionsInEpoch(scope, stream, epoch, context, executor)
                .thenApply(transactions -> transactions.entrySet().stream()
                        .filter(entry -> entry.getValue().getTxnStatus().equals(TxnStatus.COMMITTING))
                        .map(Map.Entry::getKey).collect(Collectors.toList()))
                .thenCompose(transactions -> streamMetadataStore.createTxnCommitList(scope, stream, epoch, transactions, context, executor)
                        .thenApply(x -> {
                            log.debug("Transactions {} added to commit list for epoch {} stream {}/{}", transactions, epoch, scope, stream);
                            return transactions;
                        }));
    }
}
