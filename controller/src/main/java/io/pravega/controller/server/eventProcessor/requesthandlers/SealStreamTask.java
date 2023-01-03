/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.server.eventProcessor.requesthandlers;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.shared.controller.event.SealStreamEvent;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.slf4j.LoggerFactory;

/**
 * Request handler for performing scale operations received from requeststream.
 */
public class SealStreamTask implements StreamTask<SealStreamEvent> {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(SealStreamTask.class));

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;

    public SealStreamTask(final StreamMetadataTasks streamMetadataTasks,
                          final StreamTransactionMetadataTasks streamTransactionMetadataTasks,
                          final StreamMetadataStore streamMetadataStore,
                          final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetadataStore);
        Preconditions.checkNotNull(streamMetadataTasks);
        Preconditions.checkNotNull(streamTransactionMetadataTasks);
        Preconditions.checkNotNull(executor);
        this.streamMetadataTasks = streamMetadataTasks;
        this.streamTransactionMetadataTasks = streamTransactionMetadataTasks;
        this.streamMetadataStore = streamMetadataStore;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> execute(final SealStreamEvent request) {
        Timer timer = new Timer();
        String scope = request.getScope();
        String stream = request.getStream();
        long requestId = request.getRequestId();
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);

        // when seal stream task is picked, if the state is sealing/sealed, process sealing, else postpone.
        return streamMetadataStore.getState(scope, stream, true, context, executor)
                .thenAccept(state -> {
                    if (!state.equals(State.SEALING) && !state.equals(State.SEALED)) {
                        throw new IllegalStateException("Seal stream not started.");
                    }
                })
                .thenCompose(x -> abortTransaction(context, scope, stream)
                        .thenAccept(noTransactions -> {
                            if (!noTransactions) {
                                // If transactions exist on the stream, we will throw OperationNotAllowed so that this task
                                // is retried.
                                log.debug(requestId, "Found open transactions on stream {}/{}. Postponing its sealing.",
                                        scope, stream);
                                throw StoreException.create(StoreException.Type.OPERATION_NOT_ALLOWED,
                                        "Found ongoing transactions. Abort transaction requested." +
                                                "Sealing stream segments should wait until transactions are aborted.");
                            }
                        }))
                .thenCompose(x -> streamMetadataStore.getActiveSegments(scope, stream, context, executor))
                .thenCompose(activeSegments -> {
                    if (activeSegments.isEmpty()) {
                        // idempotent check
                        // if active segment set is empty then the stream is sealed.
                        // Do not update the state if the stream is already sealed.
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return notifySealed(scope, stream, context, activeSegments, requestId);
                    }
                }).thenAccept(v -> StreamMetrics.getInstance().controllerEventProcessorSealStreamEvent(timer.getElapsed()));
    }

    /**
     * A method that issues abort request for all outstanding transactions on the stream, which are processed asynchronously.
     * This method returns false if it found transactions to abort, true otherwise.
     * @param context operation context
     * @param scope scope
     * @param stream stream
     * @return CompletableFuture which when complete will contain a boolean indicating if there are transactions of the
     * stream or not.
     */
    private CompletableFuture<Boolean> abortTransaction(OperationContext context, String scope, String stream) {
        return streamMetadataStore.getActiveTxns(scope, stream, context, executor)
                .thenCompose(activeTxns -> {
                    if (activeTxns == null || activeTxns.isEmpty()) {
                        return CompletableFuture.completedFuture(true);
                    } else {
                        Map<UUID, TxnStatus> pendingTxns = activeTxns.entrySet().stream().map(txn -> new AbstractMap.SimpleEntry<>(txn.getKey(), txn.getValue().getTxnStatus()))
                                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
                        // log list of pending txns on the Stream
                        log.info(context.getRequestId(), "Found pending transactions {} on Stream {}/{}.", pendingTxns, scope, stream);
                        return Futures.allOf(activeTxns.entrySet().stream().map(txIdPair -> {
                            CompletableFuture<Void> voidCompletableFuture;
                            TxnStatus txnStatus = txIdPair.getValue().getTxnStatus();
                            if (txnStatus.equals(TxnStatus.OPEN) || txnStatus.equals(TxnStatus.ABORTING)) {
                                // abort open transactions
                                log.trace(context.getRequestId(), "Aborting txn {} in state {}.", txIdPair.getKey(), txnStatus);
                                voidCompletableFuture = Futures.toVoid(streamTransactionMetadataTasks
                                        .abortTxn(scope, stream, txIdPair.getKey(), null, context)
                                        .exceptionally(e -> {
                                            Throwable cause = Exceptions.unwrap(e);
                                            if (cause instanceof StoreException.IllegalStateException ||
                                                    cause instanceof StoreException.WriteConflictException ||
                                                    cause instanceof StoreException.DataNotFoundException) {
                                                // IllegalStateException : The transaction is already in the process of being
                                                // completed. Ignore
                                                // WriteConflictException : Another thread is updating the transaction record.
                                                // ignore. We will effectively retry cleaning up the transaction if it is not
                                                // already being aborted.
                                                // DataNotFoundException: If transaction metadata is cleaned up after reading list
                                                // of active segments
                                                log.warn(context.getRequestId(), "A known exception thrown during seal stream " +
                                                        "while trying to abort transaction on stream {}/{}", scope, stream, cause);
                                            } else {
                                                // throw the original exception
                                                // Note: we can ignore this error because if there are transactions found on a stream,
                                                // seal stream reposts the event back into request stream.
                                                // So in subsequent iteration it will reattempt to abort all active transactions.
                                                // This is a valid course of action because it is important to understand that
                                                // all transactions are completable (either via abort of commit).
                                                log.warn(context.getRequestId(), "Exception thrown during seal stream while trying " +
                                                        "to abort transaction on stream {}/{}", scope, stream, cause);
                                            }
                                            return null;
                                        }));
                            } else {
                                voidCompletableFuture = CompletableFuture.completedFuture(null);
                            }
                            return voidCompletableFuture;
                        }).collect(Collectors.toList())).thenApply(v -> false);
                    }
                });
    }

    private CompletionStage<Void> notifySealed(String scope, String stream, OperationContext context, List<StreamSegmentRecord> activeSegments, long requestId) {
        List<Long> segmentsToBeSealed = activeSegments.stream().map(StreamSegmentRecord::segmentId).
                collect(Collectors.toList());
        log.debug(requestId, "Sending notification to segment store to seal segments for stream {}/{}", scope, stream);
        return streamMetadataTasks.notifySealedSegments(scope, stream, segmentsToBeSealed, this.streamMetadataTasks.retrieveDelegationToken(), requestId)
                                  .thenCompose(v -> setSealed(scope, stream, context));
    }

    private CompletableFuture<Void> setSealed(String scope, String stream, OperationContext context) {
        return streamMetadataStore.setSealed(scope, stream, context, executor);
    }

    @Override
    public CompletableFuture<Void> writeBack(SealStreamEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }

    @Override
    public CompletableFuture<Boolean> hasTaskStarted(SealStreamEvent event) {
        return streamMetadataStore.getState(event.getScope(), event.getStream(), true, null, executor)
                                  .thenApply(state -> state.equals(State.SEALING));
    }
}
