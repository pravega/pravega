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
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.util.Retry;
import io.pravega.controller.eventProcessor.impl.SerializedRequestHandler;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.DeleteScopeEvent;
import io.pravega.shared.controller.event.DeleteStreamEvent;
import io.pravega.shared.controller.event.StreamRequestProcessor;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import io.pravega.shared.controller.event.CreateReaderGroupEvent;
import io.pravega.shared.controller.event.DeleteReaderGroupEvent;
import io.pravega.shared.controller.event.UpdateReaderGroupEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.pravega.controller.eventProcessor.impl.EventProcessorHelper.withRetries;

/**
 * Abstract common class for all request processing done over SerializedRequestHandler.
 * This implements RequestProcessor interface and implements failing request processing for all ControllerEvent types with
 * RequestUnsupported.
 * Its derived classes should implement specific processing that they wish to handle.
 *
 * This class provides a common completion method which implements mechanisms that allow multiple event processors
 * working on same stream to get fairness in their scheduling and avoid starvation.
 * To do this, before starting any processing, it fetches waiting request processor record from the store and if there
 * is a record in the store and it doesnt match current processing request, then it simply postpones the current processing.
 * Otherwise it attempts to process the event. If the event fails in its processing because of contention with another
 * process on a different processor, then it sets itself as the waiting request processor in the stream metadata. This will
 * ensure that when other event processors complete their processing, they will not pick newer work until this processor
 * processes at least one event.
 * At the end of processing, each processor attempts to clean up waiting request processor record from the store if it
 * was set against its name.
 */
@Slf4j
public abstract class AbstractRequestProcessor<T extends ControllerEvent> extends SerializedRequestHandler<T> 
        implements StreamRequestProcessor {
    protected static final Predicate<Throwable> OPERATION_NOT_ALLOWED_PREDICATE = e -> Exceptions.unwrap(e) 
            instanceof StoreException.OperationNotAllowedException;

    protected final StreamMetadataStore streamMetadataStore;

    public AbstractRequestProcessor(StreamMetadataStore streamMetadataStore, ScheduledExecutorService executor) {
        super(executor);
        Preconditions.checkNotNull(streamMetadataStore);
        this.streamMetadataStore = streamMetadataStore;
    }

    public String getProcessorName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public CompletableFuture<Void> processEvent(ControllerEvent controllerEvent) {
        return controllerEvent.process(this);
    }

    @Override
    public CompletableFuture<Void> processAbortTxnRequest(AbortEvent abortEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    @Override
    public CompletableFuture<Void> processCommitTxnRequest(CommitEvent commitEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    @Override
    public CompletableFuture<Void> processAutoScaleRequest(AutoScaleEvent autoScaleEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    @Override
    public CompletableFuture<Void> processScaleOpRequest(ScaleOpEvent scaleOpEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    @Override
    public CompletableFuture<Void> processUpdateStream(UpdateStreamEvent updateStreamEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    @Override
    public CompletableFuture<Void> processTruncateStream(TruncateStreamEvent truncateStreamEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    @Override
    public CompletableFuture<Void> processSealStream(SealStreamEvent sealStreamEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    @Override
    public CompletableFuture<Void> processDeleteStream(DeleteStreamEvent deleteStreamEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    @Override
    public CompletableFuture<Void> processCreateReaderGroup(CreateReaderGroupEvent createRGEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    @Override
    public CompletableFuture<Void> processDeleteReaderGroup(DeleteReaderGroupEvent deleteRGEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    @Override
    public CompletableFuture<Void> processUpdateReaderGroup(UpdateReaderGroupEvent updateRGEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    @Override
    public CompletableFuture<Void> processDeleteScopeRecursive(DeleteScopeEvent deleteScopeEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    protected <T extends ControllerEvent> CompletableFuture<Void> withCompletion(StreamTask<T> task, T event, String scope, String stream,
                                                                                 Predicate<Throwable> writeBackPredicate) {
        Preconditions.checkNotNull(task);
        Preconditions.checkNotNull(event);
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(writeBackPredicate);
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        OperationContext context = streamMetadataStore.createStreamContext(scope, stream, RequestTag.NON_EXISTENT_ID);
        CompletableFuture<String> waitingProcFuture = suppressException(
                streamMetadataStore.getWaitingRequestProcessor(scope, stream, context, executor), null,
                "Exception while trying to fetch waiting request. Logged and ignored.");
        CompletableFuture<Boolean> hasTaskStarted = task.hasTaskStarted(event);
        CompletableFuture.allOf(waitingProcFuture, hasTaskStarted)
                .thenAccept(v -> {
                    boolean hasStarted = hasTaskStarted.join();
                    String waitingRequestProcessor = waitingProcFuture.join();
                    if (hasStarted || waitingRequestProcessor == null || waitingRequestProcessor.equals(getProcessorName())) {
                        withRetries(() -> task.execute(event), executor)
                                .whenComplete((r, ex) -> {
                                    if (ex != null && writeBackPredicate.test(ex)) {
                                        suppressException(streamMetadataStore.createWaitingRequestIfAbsent(scope, 
                                                stream, getProcessorName(), context, executor),
                                                null, "Exception while trying to create waiting request. Logged and ignored.")
                                                .thenCompose(ignore ->  retryIndefinitelyThenComplete(
                                                        () -> task.writeBack(event), resultFuture, ex));
                                    } else {
                                        // Processing was done for this event, whether it succeeded or failed, we should remove
                                        // the waiting request if it matches the current processor.
                                        // If we don't delete it then some other processor will never be able to do the work.
                                        // So we need to retry indefinitely until deleted.
                                        retryIndefinitelyThenComplete(
                                                () -> streamMetadataStore.deleteWaitingRequestConditionally(scope,
                                                stream, getProcessorName(), context, executor), resultFuture, ex);
                                    }
                                });
                    } else {
                        log.debug("Found another processing requested by a different processor {}. Will postpone the event {}.", 
                                waitingRequestProcessor, event);
                        // This is done to guarantee fairness. If another processor has requested for processing
                        // on this stream, we will back off and postpone the work for later.
                        retryIndefinitelyThenComplete(() -> task.writeBack(event), resultFuture,
                                StoreException.create(StoreException.Type.OPERATION_NOT_ALLOWED, "Postponed "
                                        + event + " so that waiting processor" + waitingRequestProcessor + " can work. "));
                    }
                }).exceptionally(e -> {
            resultFuture.completeExceptionally(e);
            return null;
        });

        return resultFuture;
    }

    private <R> CompletableFuture<R> suppressException(CompletableFuture<R> future, R returnOnException, String message) {
        return Futures.exceptionallyExpecting(future, e -> {
            log.warn("{}. Exception {}", message, Exceptions.unwrap(e).toString());
            return true;
        }, returnOnException);
    }

    private CompletableFuture<Void> retryIndefinitelyThenComplete(Supplier<CompletableFuture<Void>> futureSupplier, 
                                                                  CompletableFuture<Void> toComplete,
                                                                  Throwable e) {
        String failureMessage = String.format("Error writing event back into stream from processor %s", getProcessorName());
        return Retry.indefinitelyWithExpBackoff(failureMessage)
                .runAsync(futureSupplier, executor)
                .thenRun(() -> {
                    if (e != null) {
                        toComplete.completeExceptionally(e);
                    } else {
                        toComplete.complete(null);
                    }
                });
    }
}