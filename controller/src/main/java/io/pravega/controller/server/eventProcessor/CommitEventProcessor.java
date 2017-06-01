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
import io.pravega.client.stream.AckFuture;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.Retry;
import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.Position;
import io.pravega.controller.task.Stream.WriteFailedException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * This actor processes commit txn events.
 * It does the following 2 operations in order.
 * 1. Send commit txn message to active segments of the stream.
 * 2. Change txn state from committing to committed.
 */
@Slf4j
public class CommitEventProcessor extends EventProcessor<CommitEvent> {

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final ConnectionFactory connectionFactory;
    private final ScheduledExecutorService executor;
    private final SegmentHelper segmentHelper;
    private final BlockingQueue<CommitEvent> processedEvents;

    @VisibleForTesting
    public CommitEventProcessor(final StreamMetadataStore streamMetadataStore,
                                final HostControllerStore hostControllerStore,
                                final ScheduledExecutorService executor,
                                final SegmentHelper segmentHelper,
                                final ConnectionFactory connectionFactory,
                                final BlockingQueue<CommitEvent> queue) {
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
        this.connectionFactory = connectionFactory;
        this.processedEvents = queue;
    }

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

        streamMetadataStore.getActiveEpoch(scope, stream, context, executor).thenComposeAsync(pair -> {
            List<Integer> segments = pair.getRight();
            int activeEpoch = pair.getLeft();
            if (activeEpoch == epoch) {
                return completeCommit(scope, stream, epoch, segments, txnId, context);
            } else {
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
                                                   final List<Integer> segments,
                                                   final UUID txnId,
                                                   final OperationContext context) {
        return notifyCommitToHost(scope, stream, segments, txnId).thenComposeAsync(x ->
                streamMetadataStore.commitTransaction(scope, stream, epoch, txnId, context, executor), executor)
                .thenApply(x -> null);
    }

    private CompletableFuture<Void> postponeCommitEvent(CommitEvent event) {
        UUID txnId = event.getTxid();
        log.debug("Transaction {}, pushing back CommitEvent to commitStream", txnId);
        AckFuture future = this.getSelfWriter().write(event);
        CompletableFuture<AckFuture> writeComplete = new CompletableFuture<>();
        future.addListener(() -> writeComplete.complete(future), executor);
        return writeComplete.thenApplyAsync(ackFuture -> {
            try {
                // ackFuture is complete by now, so we can do a get without blocking
                ackFuture.get();
                log.debug("Transaction {}, sent request to commitStream", txnId);
                return null;
            } catch (InterruptedException e) {
                log.warn("Transaction {}, unexpected interruption sending event to commitStream. Retrying...", txnId);
                throw new WriteFailedException(e);
            } catch (ExecutionException e) {
                Throwable realException = ExceptionHelpers.getRealException(e);
                log.warn("Transaction {}, failed sending event to commitStream. Retrying...", txnId);
                throw new WriteFailedException(realException);
            }
        }, executor);
    }

    private CompletableFuture<Void> notifyCommitToHost(final String scope, final String stream,
                                                       final List<Integer> segments, final UUID txnId) {
        return FutureHelpers.allOf(segments.stream()
                .parallel()
                .map(segment -> notifyCommitToHost(scope, stream, segment, txnId))
                .collect(Collectors.toList()));
    }

    private CompletableFuture<Controller.TxnStatus> notifyCommitToHost(final String scope, final String stream,
                                                                       final int segmentNumber, final UUID txId) {
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
