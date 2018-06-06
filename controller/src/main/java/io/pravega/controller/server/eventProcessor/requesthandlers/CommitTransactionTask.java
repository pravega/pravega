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
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.CommitEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getSegmentNumber;

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

    /**
     * execute
     * 1. check if txn-commit-list exists.
     *      1.1 yes
     *      if txn-commit-list-node.epoch != request.epoch
     *          throw operationNotAllowedException
     *      else proceed to step 2
     *      1.2 no
     *      collect all txns in the same referenceepoch which are in committing state and create txn-commit-list with this set.
     * 2. set state to COMMITTING_TXN
     *      once state is set to committing, no other workflow other than current one can run. So even after failover, we
     *      will return to resume this work henceforth.
     * 3. getActiveEpoch.
     *      3.1 If txn.epoch == activeEpoch, // we can merge directly in the epoch.
     *          call commitTxnsOnActiveEpoch
     *          else commitTxnOnOldEpoch
     * 4. reset state to ACTIVE
     *
     * commitTxnsOnActiveEpoch
     * 1. loop over transaction commit list and commit transactions into active epoch one transaction at a time.
     *
     * commitTxnOnOldEpoch
     * 1. Check if rolling txn needs to be performed --> basically if all transactions are already committed,
     * we are in a rerun, do nothing and get out
     * 2. start rolling txn
     *      2.1 create duplicate txnepoch segments with secondaryId as activeepoch+1
     *      2.2 create duplicate active segments with secondaryId as activeepoch+2
     *      2.3 loop over segments and merge trasactions on segments on activeepoch+1
     *      2.4 seal activeepoch+1
     *      2.5 add to history table (activeepoch+1 in full and activeepoch+2 as partial entry)
     *      2.6 seal active epoch
     *      2.7 complete rolling txn by completing partial record in history table
     * @param event event to process
     * @return Completable future which indicates completion of processing of commit event.
     */
    @Override
    public CompletableFuture<Void> execute(CommitEvent event) {
        String scope = event.getScope();
        String stream = event.getStream();
        int epoch = event.getEpoch();
        OperationContext context = streamMetadataStore.createContext(scope, stream);
        log.debug("Attempting to commit available transactions on epoch {} on stream {}/{}", event.getEpoch(), event.getScope(), event.getStream());

        return tryCommitTransactions(scope, stream, epoch, context)
                .whenCompleteAsync((result, error) -> {
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
                                                          final int txnEpoch,
                                                          final OperationContext context) {
        // try creating txn commit list first. if node already exists and doesnt match the processing in the event, throw operation not allowed.
        // This will result in event being posted back in the stream and retried later. Generally if a transaction commit starts, it will come to
        // an end.. but during failover, once we have created the node, we are guaranteed that it will be only that transaction that will be getting
        // committed at that time.
        return streamMetadataStore.getState(scope, stream, true, context, executor)
                .thenCompose(state -> {
                    CompletableFuture<List<UUID>> txnListFuture = createRecordAndGetCommitTxnList(scope, stream, txnEpoch, context);

                    CompletableFuture<Void> commitFuture = txnListFuture
                            .thenCompose(txnList -> {
                                if (txnList == null) {
                                    // reset state conditionally in case we were left with stale committing state from a previous execution
                                    // that died just before updating the state back to ACTIVE but after having completed all the work.
                                    return streamMetadataStore.resetStateConditionally(scope, stream, State.COMMITTING_TXN, context, executor);
                                } else {
                                    // Once state is set to committing, we are guaranteed that this will be the only processing that can happen on the stream
                                    // and we can proceed with committing outstanding transactions collected in the txnList step.
                                    CompletableFuture<Void> future;
                                    // if state is sealing, we should continue with commit so that we allow for completion of transactions
                                    // in commit state.
                                    if (state.equals(State.SEALING)) {
                                        future = new CompletableFuture<>();
                                    } else {
                                        // In normal course set the state to committing before proceeding.
                                        // If we are unable to set the state to COMMITTING_TXN, it will get OPERATION_NOT_ALLOWED
                                        // and the processing will be retried later.
                                        future = Futures.toVoid(streamMetadataStore.setState(scope, stream, State.COMMITTING_TXN, context, executor));
                                    }

                                    // Note: since we have set the state to COMMITTING_TXN (or it was already sealing), the active epoch that we fetch now
                                    // cannot change until we perform rolling txn. TxnCommittingRecord ensures no other rollingTxn
                                    // can run concurrently
                                    return future.thenCompose(v -> getEpochRecords(scope, stream, txnEpoch, context)
                                            .thenCompose(records -> {
                                                HistoryRecord txnEpochRecord = records.get(0);
                                                HistoryRecord activeEpochRecord = records.get(1);
                                                if (activeEpochRecord.getEpoch() == txnEpoch ||
                                                        activeEpochRecord.getReferenceEpoch() == txnEpochRecord.getReferenceEpoch()) {
                                                    // if transactions were created on or a duplicate of active epoch,
                                                    // we can commit transactions immediately
                                                    return commitTransactions(scope, stream, activeEpochRecord.getSegments(), txnList, context);
                                                } else {
                                                    return rollTransactions(scope, stream, txnEpochRecord, activeEpochRecord, txnList, context);
                                                }
                                            }));
                                }
                            });

                    // once all commits are done, delete the committing txn record.
                    // reset state to ACTIVE if it was COMMITTING_TXN
                    return Futures.toVoid(commitFuture
                            .thenCompose(v -> streamMetadataStore.deleteCommittingTransactionsRecord(scope, stream, context, executor))
                            .thenCompose(v -> streamMetadataStore.resetStateConditionally(scope, stream, State.COMMITTING_TXN, context, executor)));
                });
    }

    private CompletableFuture<List<UUID>> createRecordAndGetCommitTxnList(String scope, String stream, int epoch, OperationContext context) {
        return streamMetadataStore.getCommittingTransactionsRecord(scope, stream, context, executor)
                .thenCompose(record -> {
                    if (record == null) {
                        // no ongoing list transactions already chosen for commit.
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

    /**
     * 1. check idempotence --> check if all transactions are already committed, then rolling txn has already happened.
     * 2. create duplicate txn epoch segments in segment store and commit transactions into those segments.
     * 3. create duplicate active epoch segments
     * 4. update history table with one complete and one partial epoch
     * 5. seal active segments
     * 6. complete partial record in history table
     */
    private CompletableFuture<Void> rollTransactions(String scope, String stream, HistoryRecord txnEpoch, HistoryRecord activeEpoch,
                                                     List<UUID> transactionsToCommit, OperationContext context) {
        // check if all transactions are already committed. if so return all good immediately
        // just checking the last one suffices as we perform processing of transactions in order.
        UUID lastTransactionId = transactionsToCommit.get(transactionsToCommit.size() - 1);
        return streamMetadataStore.transactionStatus(scope, stream, lastTransactionId, context, executor)
                .thenCompose(status -> {
                    if (status.equals(TxnStatus.COMMITTING)) {
                        return runRollingTxn(scope, stream, txnEpoch, activeEpoch, transactionsToCommit, context);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    private CompletionStage<Void> runRollingTxn(String scope, String stream, HistoryRecord txnEpoch, HistoryRecord activeEpoch,
                                                List<UUID> transactionsToCommit, OperationContext context) {
        String delegationToken = streamMetadataTasks.retrieveDelegationToken();
        long timestamp = System.currentTimeMillis();
        streamMetadataStore.getActiveEpoch(scope, stream, context, true, executor);

        int newTxnEpoch = activeEpoch.getEpoch() + 1;
        int newActieEpoch = newTxnEpoch + 1;

        List<Long> txnEpochDuplicate = txnEpoch.getSegments().stream().map(segment ->
                computeSegmentId(getSegmentNumber(segment), newTxnEpoch)).collect(Collectors.toList());
        List<Long> activeEpochDuplicate = activeEpoch.getSegments().stream()
                .map(segment -> computeSegmentId(getSegmentNumber(segment), newActieEpoch)).collect(Collectors.toList());

        return copyTxnEpochSegmentsAndCommitTxns(scope, stream, transactionsToCommit, txnEpochDuplicate, context)
                .thenCompose(v -> streamMetadataTasks.notifyNewSegments(scope, stream, activeEpochDuplicate, context, delegationToken))
                .thenCompose(v -> streamMetadataTasks.getSealedSegmentsSize(scope, stream, txnEpochDuplicate, delegationToken))
                .thenCompose(sealedSegmentsMap -> {
                    log.debug("Rolling transaction, created duplicate of active epoch {} for stream {}/{}", activeEpoch, scope, stream);
                    return streamMetadataStore.rollingTxnNewSegmentsCreated(scope, stream, sealedSegmentsMap, txnEpoch.getEpoch(), timestamp, context, executor);
                })
                .thenCompose(v -> streamMetadataTasks.notifySealedSegments(scope, stream, activeEpoch.getSegments(),
                        delegationToken))
                .thenCompose(x -> streamMetadataTasks.getSealedSegmentsSize(scope, stream, activeEpoch.getSegments(),
                        delegationToken))
                .thenCompose(sealedSegmentsMap -> {
                    log.debug("Rolling transaction, sealed active epoch {} for stream {}/{}", activeEpoch, scope, stream);
                    return streamMetadataStore.rollingTxnActiveEpochSealed(scope, stream, sealedSegmentsMap, activeEpoch.getEpoch(), timestamp, context, executor);
                });
    }

    /**
     * This method is called in the rolling transaction flow.
     * This method creates duplicate segments for transaction epoch. It then merges all transactions from the list into
     * those duplicate segments.
     */
    private CompletableFuture<Void> copyTxnEpochSegmentsAndCommitTxns(String scope, String stream, List<UUID> transactionsToCommit,
                                                                      List<Long> segmentIds, OperationContext context) {
        // 1. create duplicate segments
        // 2. merge transactions in those segments
        // 3. seal txn epoch segments
        String delegationToken = streamMetadataTasks.retrieveDelegationToken();
        CompletableFuture<Void> createSegmentsFuture = Futures.allOf(segmentIds.stream().map(segment -> {
            // Use fixed scaling policy for these segments as they are created, merged into and sealed and are not
            // supposed to auto scale.
            return streamMetadataTasks.notifyNewSegment(scope, stream, segment, ScalingPolicy.fixed(1), delegationToken);
        }).collect(Collectors.toList()));

        return createSegmentsFuture
                .thenCompose(v -> {
                    log.debug("Rolling transaction, successfully created duplicate txn epoch {} for stream {}/{}", segmentIds, scope, stream);
                    // now commit transactions into these newly created segments
                    return commitTransactions(scope, stream, segmentIds, transactionsToCommit, context);
                })
                .thenCompose(v -> streamMetadataTasks.notifySealedSegments(scope, stream, segmentIds, delegationToken));
    }

    /**
     * This method loops over each transaction in the list, commits them by calling into segment store followed by marking
     * the transaction metadata for completion. Finally it will remove the from the txn commit list.
     * At the end of this method's execution, all transactions in the list would have committed and committing list in the store
     * would become empty.
     */
    private CompletableFuture<Void> commitTransactions(String scope, String stream, List<Long> segments,
                                                       List<UUID> transactionsToCommit, OperationContext context) {
        // Chain all transaction commit futures one after the other. This will ensure that order of commit
        // if honoured and is based on the order in the list.
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        for (UUID txnId : transactionsToCommit) {
            log.debug("Committing transaction {} on stream {}/{}", txnId, scope, stream);
            // commit transaction in segment store
            future = future
                    // Important: seal transaction segment. Note, we can use the same segments and transaction id as only
                    // primary id is taken for creation of txn-segment name and secondary part is erased.
                    // And we are creating duplicates of txn epoch keeping the primary same.
                    .thenCompose(v -> streamMetadataTasks.notifyTxnSeal(scope, stream, segments, txnId))
                    .thenCompose(v -> streamMetadataTasks.notifyTxnCommit(scope, stream, segments, txnId))
                    // mark transaction as committed in metadata store.
                    .thenCompose(x -> streamMetadataStore.commitTransaction(scope, stream, txnId, context, executor)
                            .thenAccept(done -> {
                                log.debug("transaction {} on stream {}/{} committed successfully", txnId, scope, stream);
                            }));
        }
        return future;
    }

    /**
     * Get transactions in epoch. If no transactions exist return null.
     * If transactions exist, create a new Committing transactions record in the store.
     * Note, before calling this method, we check if committingTxnList exists or not so we can never get DataExistsException.
     */
    private CompletableFuture<List<UUID>> createNewTxnCommitList(String scope, String stream, int epoch,
                                                                 OperationContext context, ScheduledExecutorService executor) {
        return streamMetadataStore.getTransactionsInEpoch(scope, stream, epoch, context, executor)
                .thenApply(transactions -> transactions.entrySet().stream()
                        .filter(entry -> entry.getValue().getTxnStatus().equals(TxnStatus.COMMITTING))
                        .map(Map.Entry::getKey).collect(Collectors.toList()))
                .thenCompose(transactions -> {
                    if (!transactions.isEmpty()) {
                        return streamMetadataStore.createCommittingTransactionsRecord(scope, stream, epoch, transactions, context, executor)
                                .thenApply(x -> {
                                    log.debug("Transactions {} added to commit list for epoch {} stream {}/{}", transactions, epoch, scope, stream);
                                    return transactions;
                                });
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    /**
     * Fetches epoch history records for active epoch and the supplied `epoch` from the store.
     */
    private CompletableFuture<List<HistoryRecord>> getEpochRecords(String scope, String stream, int epoch, OperationContext context) {
        List<CompletableFuture<HistoryRecord>> list = new ArrayList<>();
        list.add(streamMetadataStore.getEpoch(scope, stream, epoch, context, executor));
        list.add(streamMetadataStore.getActiveEpoch(scope, stream, context, true, executor));
        return Futures.allOfWithResults(list);
    }
}
