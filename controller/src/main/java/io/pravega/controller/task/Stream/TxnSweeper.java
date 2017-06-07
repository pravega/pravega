/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task.Stream;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.task.TxnResource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Sweeper for transactions orphaned by failed controller processes.
 */
@Slf4j
public class TxnSweeper {
    private final StreamMetadataStore streamMetadataStore;
    private final StreamTransactionMetadataTasks transactionMetadataTasks;
    private final long maxTxnTimeoutMillis;
    private final ScheduledExecutorService executor;

    @Data
    private static class Result {
        private final TxnResource txnResource;
        private final Object value;
        private final Throwable error;
    }

    public TxnSweeper(final StreamMetadataStore streamMetadataStore,
                      final StreamTransactionMetadataTasks transactionMetadataTasks,
                      final long maxTxnTimeoutMillis,
                      final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetadataStore, "streamMetadataStore");
        Preconditions.checkNotNull(transactionMetadataTasks, "transactionMetadataTasks");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkArgument(maxTxnTimeoutMillis > 0, "maxTxnTimeoutMillis should be a positive number");

        this.streamMetadataStore = streamMetadataStore;
        this.transactionMetadataTasks = transactionMetadataTasks;
        this.maxTxnTimeoutMillis = maxTxnTimeoutMillis;
        this.executor = executor;
    }

    public boolean isReady() {
        return transactionMetadataTasks.isReady();
    }

    public void awaitInitialization() throws InterruptedException {
        transactionMetadataTasks.awaitInitialization();
    }

    public CompletableFuture<Void> sweepFailedHosts(Supplier<Set<String>> activeHosts) {
        if (!isReady()) {
            return FutureHelpers.failedFuture(new IllegalStateException(getClass().getName() + " not yet ready"));
        }
        return streamMetadataStore.listHostsOwningTxn().thenComposeAsync(index -> {
            index.removeAll(activeHosts.get());
            log.info("Failed hosts {} have orphaned tasks", index);
            return FutureHelpers.allOf(index.stream().map(this::sweepOrphanedTxns).collect(Collectors.toList()));
        }, executor);
    }

    public CompletableFuture<Void> sweepOrphanedTxns(String failedHost) {
        if (!isReady()) {
            return FutureHelpers.failedFuture(new IllegalStateException(getClass().getName() + " not yet ready"));
        }
        log.info("Host={}, sweeping orphaned transactions", failedHost);
        return FutureHelpers.delayedFuture(Duration.ofMillis(maxTxnTimeoutMillis), executor)
                .thenComposeAsync(ignore ->
                        FutureHelpers.doWhileLoop(() -> failOverTxns(failedHost), x -> x != null, executor), executor)
                .whenCompleteAsync((v, e) ->
                        log.info("Host={}, sweeping orphaned transactions complete", failedHost), executor);
    }

    private CompletableFuture<Result> failOverTxns(String failedHost) {
        return streamMetadataStore.getRandomTxnFromIndex(failedHost).thenComposeAsync(resourceOpt -> {
            if (resourceOpt.isPresent()) {
                TxnResource resource = resourceOpt.get();
                // Get transaction's status
                // If it is aborting or committing, then send an abortEvent or commitEvent to respective streams.
                // Else, if it is open, then try to abort it.
                // Else, ignore it.
                return failOverTxn(failedHost, resource);
            } else {
                // delete hostId from the index.
                return streamMetadataStore.removeHostFromIndex(failedHost).thenApplyAsync(x -> null, executor);
            }
        }, executor);
    }

    private CompletableFuture<Result> failOverTxn(String failedHost, TxnResource txn) {
        String scope = txn.getScope();
        String stream = txn.getStream();
        UUID txnId = txn.getTxnId();
        log.debug("Host = {}, processing transaction {}/{}/{}", failedHost, scope, stream, txnId);
        return streamMetadataStore.getTransactionData(scope, stream, txnId, null, executor).thenComposeAsync(txData -> {
            int epoch = txData.getEpoch();
            switch (txData.getStatus()) {
                case OPEN:
                    return failOverOpenTxn(failedHost, txn)
                            .handleAsync((v, e) -> new Result(txn, v, e), executor);
                case ABORTING:
                    return failOverAbortingTxn(failedHost, epoch, txn)
                            .handleAsync((v, e) -> new Result(txn, v, e), executor);
                case COMMITTING:
                    return failOverCommittingTxn(failedHost, epoch, txn)
                            .handleAsync((v, e) -> new Result(txn, v, e), executor);
                default:
                    return CompletableFuture.completedFuture(new Result(txn, null, null));
            }
        }, executor).whenComplete((v, e) ->
                log.debug("Host = {}, processing transaction {}/{}/{} complete", failedHost, scope, stream, txnId));
    }

    private CompletableFuture<Void> failOverCommittingTxn(String failedHost, int epoch, TxnResource txn) {
        String scope = txn.getScope();
        String stream = txn.getStream();
        UUID txnId = txn.getTxnId();
        log.debug("Host = {}, failing over committing transaction {}/{}/{}", failedHost, scope, stream, txnId);
        return transactionMetadataTasks.writeCommitEvent(scope, stream, epoch, txnId, TxnStatus.COMMITTING)
                .thenComposeAsync(status -> streamMetadataStore.removeTxnFromIndex(failedHost, txn, true), executor);
    }

    private CompletableFuture<Void> failOverAbortingTxn(String failedHost, int epoch, TxnResource txn) {
        String scope = txn.getScope();
        String stream = txn.getStream();
        UUID txnId = txn.getTxnId();
        log.debug("Host = {}, failing over aborting transaction {}/{}/{}", failedHost, scope, stream, txnId);
        return transactionMetadataTasks.writeAbortEvent(scope, stream, epoch, txnId, TxnStatus.ABORTING)
                .thenComposeAsync(status -> streamMetadataStore.removeTxnFromIndex(failedHost, txn, true), executor);
    }

    private CompletableFuture<Void> failOverOpenTxn(String failedHost, TxnResource txn) {
        String scope = txn.getScope();
        String stream = txn.getStream();
        UUID txnId = txn.getTxnId();
        log.debug("Host = {}, failing over open transaction {}/{}/{}", failedHost, scope, stream, txnId);
        return streamMetadataStore.getTxnVersionFromIndex(failedHost, txn).thenComposeAsync((Integer version) ->
                transactionMetadataTasks.abortTxnBody(failedHost, scope, stream, txnId, version, null)
                        .thenApplyAsync(status -> null, executor), executor);
    }
}
