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

import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.AbortEvent;
import io.pravega.controller.server.eventProcessor.CommitEvent;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.client.stream.AckFuture;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.store.task.TxnResource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
@Slf4j
public class StreamTransactionMetadataTasks implements AutoCloseable {

    protected EventStreamWriter<CommitEvent> commitEventEventStreamWriter;
    protected EventStreamWriter<AbortEvent> abortEventEventStreamWriter;
    protected String commitStreamName;
    protected String abortStreamName;
    protected final String hostId;
    protected final ScheduledExecutorService executor;

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final SegmentHelper segmentHelper;
    private final ConnectionFactory connectionFactory;

    private volatile boolean ready;
    private final CountDownLatch readyLatch;

    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final HostControllerStore hostControllerStore,
                                          final SegmentHelper segmentHelper,
                                          final ScheduledExecutorService executor,
                                          final String hostId,
                                          final ConnectionFactory connectionFactory) {
        this.hostId = hostId;
        this.executor = executor;
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.connectionFactory = connectionFactory;
        readyLatch = new CountDownLatch(1);
    }

    protected void setReady() {
        ready = true;
        readyLatch.countDown();
    }

    public boolean isReady() {
        return ready;
    }

    @VisibleForTesting
    public boolean awaitInitialization(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return readyLatch.await(timeout, timeUnit);
    }

    public void awaitInitialization() throws InterruptedException {
        readyLatch.await();
    }

    /**
     * Initializes stream writers for commit and abort streams.
     * This method should be called immediately after creating StreamTransactionMetadataTasks object.
     *
     * @param clientFactory Client factory reference.
     * @param config Controller event processor configuration.
     */
    public Void initializeStreamWriters(final ClientFactory clientFactory,
                                        final ControllerEventProcessorConfig config) {
        this.commitStreamName = config.getCommitStreamName();
        this.commitEventEventStreamWriter = clientFactory.createEventWriter(
                config.getCommitStreamName(),
                ControllerEventProcessors.COMMIT_EVENT_SERIALIZER,
                EventWriterConfig.builder().build());

        this.abortStreamName = config.getAbortStreamName();
        this.abortEventEventStreamWriter = clientFactory.createEventWriter(
                config.getAbortStreamName(),
                ControllerEventProcessors.ABORT_EVENT_SERIALIZER,
                EventWriterConfig.builder().build());

        this.setReady();
        return null;
    }

    @VisibleForTesting
    public Void initializeStreamWriters(final String commitStreamName, final EventStreamWriter<CommitEvent> commitWriter,
                                 final String abortStreamName, final EventStreamWriter<AbortEvent> abortWriter) {
        this.commitStreamName = commitStreamName;
        this.commitEventEventStreamWriter = commitWriter;
        this.abortStreamName = abortStreamName;
        this.abortEventEventStreamWriter = abortWriter;
        this.setReady();
        return null;
    }

    /**
     * Create transaction.
     *
     * @param scope            stream scope.
     * @param stream           stream name.
     * @param lease            Time for which transaction shall remain open with sending any heartbeat.
     * @param maxExecutionTime Maximum time for which client may extend txn lease.
     * @param scaleGracePeriod Maximum time for which client may extend txn lease once
     *                         the scaling operation is initiated on the txn stream.
     * @param ctxOpt           operational context
     * @return transaction id.
     */
    public CompletableFuture<Pair<VersionedTransactionData, List<Segment>>> createTxn(final String scope,
                                                                                      final String stream,
                                                                                      final long lease,
                                                                                      final long maxExecutionTime,
                                                                                      final long scaleGracePeriod,
                                                                                      final OperationContext ctxOpt) {
        return checkReady().thenComposeAsync(x -> {
            final OperationContext context =
                    ctxOpt == null ? streamMetadataStore.createContext(scope, stream) : ctxOpt;
            return createTxnBody(scope, stream, lease, maxExecutionTime, scaleGracePeriod, context);
        }, executor);
    }

    /**
     * Transaction heartbeat, that increases transaction timeout by lease number of milliseconds.
     *
     * @param scope Stream scope.
     * @param stream Stream name.
     * @param txId Transaction identifier.
     * @param lease Amount of time in milliseconds by which to extend the transaction lease.
     * @param contextOpt       operational context
     * @return Transaction metadata along with the version of it record in the store.
     */
    public CompletableFuture<VersionedTransactionData> pingTxn(final String scope, final String stream,
                                                               final UUID txId, final long lease,
                                                               final OperationContext contextOpt) {
        return checkReady().thenComposeAsync(x -> {
            final OperationContext context =
                    contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
            return pingTxnBody(scope, stream, txId, lease, context);
        }, executor);
    }

    /**
     * Abort transaction.
     *
     * @param scope  stream scope.
     * @param stream stream name.
     * @param txId   transaction id.
     * @param version Expected version of the transaction record in the store.
     * @param contextOpt       operational context
     * @return true/false.
     */
    public CompletableFuture<TxnStatus> abortTxn(final String scope, final String stream, final UUID txId,
                                                 final Integer version, final OperationContext contextOpt) {
        return checkReady().thenComposeAsync(x -> {
            final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
            return abortTxnBody(hostId, scope, stream, txId, version, context);
        }, executor);
    }

    /**
     * Commit transaction.
     *
     * @param scope      stream scope.
     * @param stream     stream name.
     * @param txId       transaction id.
     * @param contextOpt optional context
     * @return true/false.
     */
    public CompletableFuture<TxnStatus> commitTxn(final String scope, final String stream, final UUID txId,
                                                  final OperationContext contextOpt) {
        return checkReady().thenComposeAsync(x -> {
            final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
            return commitTxnBody(hostId, scope, stream, txId, context);
        }, executor);
    }

    private CompletableFuture<Void> checkReady() {
        if (!ready) {
            return FutureHelpers.failedFuture(new IllegalStateException(getClass().getName() + " not yet ready"));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Pair<VersionedTransactionData, List<Segment>>> createTxnBody(final String scope,
                                                                                           final String stream,
                                                                                           final long lease,
                                                                                           final long maxExecutionPeriod,
                                                                                           final long scaleGracePeriod,
                                                                                           final OperationContext ctx) {
        UUID txnId = UUID.randomUUID();
        TxnResource resource = new TxnResource(scope, stream, txnId);
        CompletableFuture<Void> addIndex = streamMetadataStore.addTxnToIndex(hostId, resource, 0);
        return addIndex.thenComposeAsync(x -> streamMetadataStore.createTransaction(scope, stream, txnId, lease,
                maxExecutionPeriod, scaleGracePeriod, ctx, executor).thenComposeAsync(txData ->
                streamMetadataStore.getActiveSegments(scope, stream, ctx, executor).thenComposeAsync(activeSegments ->
                        notifyTxCreation(scope, stream, activeSegments, txnId).thenApply(v ->
                                new ImmutablePair<>(txData, activeSegments)), executor), executor), executor);
    }

    private CompletableFuture<VersionedTransactionData> pingTxnBody(final String scope, final String stream,
                                                                    final UUID txId, long lease,
                                                                    final OperationContext ctx) {
        return streamMetadataStore.pingTransaction(scope, stream, txId, lease, ctx, executor);
    }

    public CompletableFuture<TxnStatus> abortTxnBody(final String host,
                                                      final String scope,
                                                      final String stream,
                                                      final UUID txid,
                                                      final Integer version,
                                                      final OperationContext ctx) {
        TxnResource resource = new TxnResource(scope, stream, txid);
        return streamMetadataStore.sealTransaction(scope, stream, txid, false, Optional.ofNullable(version), ctx, executor)
                .thenComposeAsync(status -> {
                    if (status == TxnStatus.ABORTING) {
                        return writeAbortEvent(scope, stream, txid, status);
                    } else {
                        // Status is ABORTED, return it.
                        return CompletableFuture.completedFuture(status);
                    }
                }, executor).thenComposeAsync(status ->
                        streamMetadataStore.removeTxnFromIndex(host, resource, true).thenApply(x -> status), executor);
    }

    public CompletableFuture<TxnStatus> commitTxnBody(final String host,
                                                       final String scope,
                                                       final String stream,
                                                       final UUID txid,
                                                       final OperationContext ctx) {
        TxnResource resource = new TxnResource(scope, stream, txid);
        return streamMetadataStore.sealTransaction(scope, stream, txid, true, Optional.empty(), ctx, executor)
                .thenComposeAsync(status -> {
                    if (status == TxnStatus.COMMITTING) {
                        return writeCommitEvent(scope, stream, txid, status);
                    } else {
                        // Status is COMMITTED, return it.
                        return CompletableFuture.completedFuture(status);
                    }
                }, executor).thenComposeAsync(status ->
                        streamMetadataStore.removeTxnFromIndex(host, resource, true).thenApply(x -> status), executor);
    }

    public CompletableFuture<TxnStatus> writeCommitEvent(String scope, String stream, UUID txnId, TxnStatus status) {
        String key = scope + stream;
        CommitEvent event = new CommitEvent(scope, stream, txnId);
        return writeEventWithRetries(commitEventEventStreamWriter, commitStreamName, key, event, txnId, status);
    }

    public CompletableFuture<TxnStatus> writeAbortEvent(String scope, String stream, UUID txnId, TxnStatus status) {
        String key = txnId.toString();
        AbortEvent event = new AbortEvent(scope, stream, txnId);
        return writeEventWithRetries(abortEventEventStreamWriter, abortStreamName, key, event, txnId, status);
    }

    private <T> CompletableFuture<TxnStatus> writeEventWithRetries(final EventStreamWriter<T> streamWriter,
                                                                   final String streamName,
                                                                   final String key,
                                                                   final T event,
                                                                   final UUID txid,
                                                                   final TxnStatus txnStatus) {
        return TaskStepsRetryHelper.withRetries(() -> writeEvent(streamWriter, streamName, key, event, txid, txnStatus),
                executor);
    }

    private <T> CompletableFuture<TxnStatus> writeEvent(final EventStreamWriter<T> streamWriter,
                                                        final String streamName,
                                                        final String key,
                                                        final T event,
                                                        final UUID txid,
                                                        final TxnStatus txnStatus) {
        log.debug("Transaction {}, state={}, sending request to {}", txid, txnStatus, streamName);
        AckFuture future = streamWriter.writeEvent(key, event);
        CompletableFuture<AckFuture> writeComplete = new CompletableFuture<>();
        future.addListener(() -> writeComplete.complete(future), executor);
        return writeComplete.thenApplyAsync(ackFuture -> {
            try {
                // ackFuture is complete by now, so we can do a get without blocking
                ackFuture.get();
                log.debug("Transaction {}, sent request to {}", txid, streamName);
                return txnStatus;
            } catch (InterruptedException e) {
                log.warn("Transaction {}, unexpected interrupted exception while sending {} to {}. Retrying...",
                        txid, event.getClass().getSimpleName(), streamName);
                throw new WriteFailedException(e);
            } catch (ExecutionException e) {
                Throwable realException = ExceptionHelpers.getRealException(e);
                log.warn("Transaction {}, failed sending {} to {}. Retrying...",
                        txid, event.getClass().getSimpleName(), streamName);
                throw new WriteFailedException(realException);
            }
        }, executor);
    }

    private CompletableFuture<Void> notifyTxCreation(final String scope,
                                                     final String stream,
                                                     final List<Segment> activeSegments,
                                                     final UUID txnId) {
        return FutureHelpers.allOf(activeSegments.stream()
                .parallel()
                .map(segment -> notifyTxCreation(scope, stream, segment.getNumber(), txnId))
                .collect(Collectors.toList()));
    }

    private CompletableFuture<UUID> notifyTxCreation(final String scope,
                                                     final String stream,
                                                     final int segmentNumber,
                                                     final UUID txid) {
        return TaskStepsRetryHelper.withRetries(() -> segmentHelper.createTransaction(scope,
                stream,
                segmentNumber,
                txid,
                this.hostControllerStore,
                this.connectionFactory), executor);
    }

    @Override
    public void close() throws Exception {
        if (commitEventEventStreamWriter != null) {
            commitEventEventStreamWriter.close();
        }
        if (abortEventEventStreamWriter != null) {
            abortEventEventStreamWriter.close();
        }
    }
}
