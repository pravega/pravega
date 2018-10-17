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
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.fault.FailoverSweeper;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.task.TxnResource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.util.RetryHelper.RETRYABLE_PREDICATE;
import static io.pravega.controller.util.RetryHelper.withRetriesAsync;

/**
 * Sweeper for transactions orphaned by failed controller processes.
 */
@Slf4j
public class TxnSweeper implements FailoverSweeper {

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

    public void awaitInitialization() throws InterruptedException {
        transactionMetadataTasks.awaitInitialization();
    }

    @Override
    public boolean isReady() {
        return transactionMetadataTasks.isReady();
    }

    @Override
    public CompletableFuture<Void> sweepFailedProcesses(Supplier<Set<String>> activeHosts) {
        if (!transactionMetadataTasks.isReady()) {
            return Futures.failedFuture(new IllegalStateException(getClass().getName() + " not yet ready"));
        }
        CompletableFuture<Set<String>> hostsOwningTxns = withRetriesAsync(streamMetadataStore::listHostsOwningTxn,
                RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor);
        return hostsOwningTxns.thenComposeAsync(index -> {
            index.removeAll(activeHosts.get());
            log.info("Failed hosts {} have orphaned tasks", index);
            return Futures.allOf(index.stream().map(this::handleFailedProcess).collect(Collectors.toList()));
        }, executor);
    }

    @Override
    public CompletableFuture<Void> handleFailedProcess(String failedHost) {
        if (!transactionMetadataTasks.isReady()) {
            return Futures.failedFuture(new IllegalStateException(getClass().getName() + " not yet ready"));
        }
        log.info("Host={}, sweeping orphaned transactions", failedHost);
        CompletableFuture<Void> delay = Futures.delayedFuture(Duration.ofMillis(2 * maxTxnTimeoutMillis), executor);
        return delay.thenComposeAsync(x -> withRetriesAsync(() -> sweepOrphanedTxnsWithoutDelay(failedHost),
                RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor));
    }

    private CompletableFuture<Void> sweepOrphanedTxnsWithoutDelay(String failedHost) {
        CompletableFuture<Void> failOverTxns = Futures.doWhileLoop(() -> failOverTxns(failedHost),
                x -> x != null, executor);
        return failOverTxns.whenCompleteAsync((v, e) -> {
            if (e != null) {
                log.warn("Host={}, Caught exception sweeping orphaned transactions", failedHost, e);
            } else {
                log.debug("Host={}, sweeping orphaned transactions complete", failedHost);
            }
        }, executor);
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
        return streamMetadataStore.getTransactionData(scope, stream, txnId, null, executor).handle((r, e) -> {
            if (e != null) {
                if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                    // transaction not found, which means it should already have completed. We will ignore such txns
                    return VersionedTransactionData.EMPTY;
                } else {
                    throw new CompletionException(e);
                }
            }
            return r;
        }).thenComposeAsync(txData -> {
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
                case UNKNOWN:
                default:
                    return streamMetadataStore.removeTxnFromIndex(failedHost, txn, true)
                            .thenApply(x -> new Result(txn, null, null));
            }
        }, executor).whenComplete((v, e) ->
                log.debug("Host = {}, processing transaction {}/{}/{} complete", failedHost, scope, stream, txnId));
    }

    private CompletableFuture<Void> failOverCommittingTxn(String failedHost, int epoch, TxnResource txn) {
        String scope = txn.getScope();
        String stream = txn.getStream();
        UUID txnId = txn.getTxnId();
        // To persist the original requestId across hosts in TxnSweeper would require to add requestID in TxnResource.
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "commitTransaction",
                txn.getScope(), txn.getStream(), txn.getTxnId().toString());
        LoggerHelpers.debugLogWithTag(log, requestTag.getRequestId(), "Host = {}, failing over committing transaction {}/{}/{}",
                failedHost, scope, stream, txnId);
        return transactionMetadataTasks.writeCommitEvent(scope, stream, epoch, txnId, TxnStatus.COMMITTING, requestTag.getRequestId())
                .thenComposeAsync(status -> streamMetadataStore.removeTxnFromIndex(failedHost, txn, true), executor);
    }

    private CompletableFuture<Void> failOverAbortingTxn(String failedHost, int epoch, TxnResource txn) {
        String scope = txn.getScope();
        String stream = txn.getStream();
        UUID txnId = txn.getTxnId();
        // To persist the original requestId across hosts in TxnSweeper would require to add requestID in TxnResource.
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "abortTransaction",
                txn.getScope(), txn.getStream(), txn.getTxnId().toString());
        LoggerHelpers.debugLogWithTag(log, requestTag.getRequestId(), "Host = {}, failing over aborting transaction {}/{}/{}",
                failedHost, scope, stream, txnId);
        return transactionMetadataTasks.writeAbortEvent(scope, stream, epoch, txnId, TxnStatus.ABORTING, requestTag.getRequestId())
                .thenComposeAsync(status -> streamMetadataStore.removeTxnFromIndex(failedHost, txn, true), executor);
    }

    private CompletableFuture<Void> failOverOpenTxn(String failedHost, TxnResource txn) {
        String scope = txn.getScope();
        String stream = txn.getStream();
        UUID txnId = txn.getTxnId();
        // To persist the original requestId across hosts in TxnSweeper would require to add requestID in TxnResource.
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "openTransaction",
                txn.getScope(), txn.getStream(), txn.getTxnId().toString());
        LoggerHelpers.debugLogWithTag(log, requestTag.getRequestId(), "Host = {}, failing over open transaction {}/{}/{}",
                failedHost, scope, stream, txnId);
        return streamMetadataStore.getTxnVersionFromIndex(failedHost, txn).thenComposeAsync((Integer version) ->
                transactionMetadataTasks.sealTxnBody(failedHost, scope, stream, false, txnId, version, null, requestTag.getRequestId())
                        .thenApplyAsync(status -> null, executor), executor);
    }
}
