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
import io.pravega.controller.store.task.Resource;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.task.Task;
import io.pravega.controller.task.TaskBase;
import io.pravega.client.stream.AckFuture;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
@Slf4j
public class StreamTransactionMetadataTasks extends TaskBase {

    protected EventStreamWriter<CommitEvent> commitEventEventStreamWriter;
    protected EventStreamWriter<AbortEvent> abortEventEventStreamWriter;
    protected String commitStreamName;
    protected String abortStreamName;

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final SegmentHelper segmentHelper;
    private final ConnectionFactory connectionFactory;

    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final HostControllerStore hostControllerStore,
                                          final TaskMetadataStore taskMetadataStore,
                                          final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                                          final String hostId,
                                          final ConnectionFactory connectionFactory) {
        this(streamMetadataStore, hostControllerStore, taskMetadataStore, segmentHelper, executor, new Context(hostId), connectionFactory);
    }

    private StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                           final HostControllerStore hostControllerStore,
                                           final TaskMetadataStore taskMetadataStore,
                                           SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                                           final Context context,
                                           final ConnectionFactory connectionFactory) {
        super(taskMetadataStore, executor, context);
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.connectionFactory = connectionFactory;
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
     * @param contextOpt       operational context
     * @return transaction id.
     */
    @Task(name = "createTransaction", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<Pair<VersionedTransactionData, List<Segment>>> createTxn(final String scope,
                                                                                      final String stream,
                                                                                      final long lease,
                                                                                      final long maxExecutionTime,
                                                                                      final long scaleGracePeriod,
                                                                                      final OperationContext contextOpt) {
        final OperationContext context = getNonNullOperationContext(scope, stream, contextOpt);
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, lease, maxExecutionTime, scaleGracePeriod, null},
                () -> createTxnBody(scope, stream, lease, maxExecutionTime, scaleGracePeriod, context));
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
        final OperationContext context = getNonNullOperationContext(scope, stream, contextOpt);
        return execute(
                new Resource(scope, stream, txId.toString()),
                new Serializable[]{scope, stream, txId},
                () -> pingTxnBody(scope, stream, txId, lease, context));
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
    @Task(name = "abortTransaction", version = "1.0", resource = "{scope}/{stream}/{txId}")
    public CompletableFuture<TxnStatus> abortTxn(final String scope, final String stream, final UUID txId,
                                                 final Integer version, final OperationContext contextOpt) {
        final OperationContext context = getNonNullOperationContext(scope, stream, contextOpt);
        return execute(
                new Resource(scope, stream, txId.toString()),
                new Serializable[]{scope, stream, txId, version, null},
                () -> abortTxnBody(scope, stream, txId, version, context));
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
    @Task(name = "commitTransaction", version = "1.0", resource = "{scope}/{stream}/{txId}")
    public CompletableFuture<TxnStatus> commitTxn(final String scope, final String stream, final UUID txId,
                                                  final OperationContext contextOpt) {
        final OperationContext context = getNonNullOperationContext(scope, stream, contextOpt);

        return execute(
                new Resource(scope, stream, txId.toString()),
                new Serializable[]{scope, stream, txId, null},
                () -> commitTxnBody(scope, stream, txId, context));
    }

    private OperationContext getNonNullOperationContext(final String scope,
                                                        final String stream,
                                                        final OperationContext contextOpt) {
        return contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
    }

    private CompletableFuture<Pair<VersionedTransactionData, List<Segment>>> createTxnBody(final String scope,
                                                                                           final String stream,
                                                                                           final long lease,
                                                                                           final long maxExecutionPeriod,
                                                                                           final long scaleGracePeriod,
                                                                                           final OperationContext ctx) {
        UUID txnId = UUID.randomUUID();
        return streamMetadataStore.createTransaction(scope, stream, txnId, lease, maxExecutionPeriod,
                scaleGracePeriod, ctx, executor)
                .thenComposeAsync(txData -> streamMetadataStore.getActiveSegments(scope, stream, ctx, executor)
                        .thenComposeAsync(segments -> notifyTxnCreation(scope, stream, segments, txnId)
                                .thenApplyAsync(v -> new ImmutablePair<>(txData, segments), executor), executor), executor);
    }

    private CompletableFuture<VersionedTransactionData> pingTxnBody(String scope, String stream, UUID txId, long lease,
                                                                    final OperationContext context) {
        return streamMetadataStore.pingTransaction(scope, stream, txId, lease, context, executor);
    }

    private CompletableFuture<TxnStatus> abortTxnBody(final String scope, final String stream, final UUID txnId,
                                                      final Integer version, final OperationContext ctx) {
        Optional<Integer> versionOpt = Optional.ofNullable(version);
        return streamMetadataStore.sealTransaction(scope, stream, txnId, false, versionOpt, ctx, executor)
                .thenComposeAsync(pair -> {
                    TxnStatus status = pair.getKey();
                    if (status == TxnStatus.ABORTING) {
                        int epoch = pair.getValue();
                        String key = txnId.toString();
                        AbortEvent event = new AbortEvent(scope, stream, epoch, txnId);
                        return TaskStepsRetryHelper.withRetries(() -> writeEvent(abortEventEventStreamWriter,
                                abortStreamName, key, event, txnId, status), executor);
                    } else {
                        // Status is ABORTED, return it.
                        return CompletableFuture.completedFuture(status);
                    }
                }, executor);
    }

    private CompletableFuture<TxnStatus> commitTxnBody(final String scope, final String stream, final UUID txnId,
                                                       final OperationContext context) {
        return streamMetadataStore.sealTransaction(scope, stream, txnId, true, Optional.empty(), context, executor)
                .thenComposeAsync(pair -> {
                    TxnStatus status = pair.getKey();
                    if (status == TxnStatus.COMMITTING) {
                        int epoch = pair.getValue();
                        String key = scope + stream;
                        CommitEvent event = new CommitEvent(scope, stream, epoch, txnId);
                        return TaskStepsRetryHelper.withRetries(() -> writeEvent(commitEventEventStreamWriter,
                                commitStreamName, key, event, txnId, status), executor);
                    } else {
                        // Status is COMMITTED, return it.
                        return CompletableFuture.completedFuture(status);
                    }
                }, executor);
    }

    private <T> CompletableFuture<TxnStatus> writeEvent(final EventStreamWriter<T> streamWriter,
                                                        final String streamName,
                                                        final String key,
                                                        final T event,
                                                        final UUID txnId,
                                                        final TxnStatus txnStatus) {
        log.debug("Transaction {}, state={}, sending request to {}", txnId, txnStatus, streamName);
        AckFuture future = streamWriter.writeEvent(key, event);
        CompletableFuture<AckFuture> writeComplete = new CompletableFuture<>();
        future.addListener(() -> writeComplete.complete(future), executor);
        return writeComplete.thenApplyAsync(ackFuture -> {
            try {
                // ackFuture is complete by now, so we can do a get without blocking
                ackFuture.get();
                log.debug("Transaction {}, sent request to {}", txnId, streamName);
                return txnStatus;
            } catch (InterruptedException e) {
                log.warn("Transaction {}, unexpected interrupted exception while sending {} to {}. Retrying...",
                        txnId, event.getClass().getSimpleName(), streamName);
                throw new WriteFailedException(e);
            } catch (ExecutionException e) {
                Throwable realException = ExceptionHelpers.getRealException(e);
                log.warn("Transaction {}, failed sending {} to {}. Retrying...",
                        txnId, event.getClass().getSimpleName(), streamName);
                throw new WriteFailedException(realException);
            }
        }, executor);
    }

    private CompletableFuture<Void> notifyTxnCreation(final String scope, final String stream,
                                                      final List<Segment> segments, final UUID txnId) {
        return FutureHelpers.allOf(segments.stream()
                .parallel()
                .map(segment -> notifyTxCreation(scope, stream, segment.getNumber(), txnId))
                .collect(Collectors.toList()));
    }

    private CompletableFuture<UUID> notifyTxCreation(final String scope, final String stream,
                                                     final int segmentNumber, final UUID txnId) {
        return TaskStepsRetryHelper.withRetries(() -> segmentHelper.createTransaction(scope,
                stream,
                segmentNumber,
                txnId,
                this.hostControllerStore,
                this.connectionFactory), executor);
    }

    @Override
    public TaskBase copyWithContext(Context context) {
        StreamTransactionMetadataTasks transactionMetadataTasks =
                new StreamTransactionMetadataTasks(streamMetadataStore,
                        hostControllerStore,
                        taskMetadataStore,
                        segmentHelper, executor,
                        context,
                        connectionFactory);
        if (this.isReady()) {
            transactionMetadataTasks.commitStreamName = this.commitStreamName;
            transactionMetadataTasks.commitEventEventStreamWriter = this.commitEventEventStreamWriter;
            transactionMetadataTasks.abortStreamName = this.abortStreamName;
            transactionMetadataTasks.abortEventEventStreamWriter = this.abortEventEventStreamWriter;
            transactionMetadataTasks.setReady();
        }
        return transactionMetadataTasks;
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
