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

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.controller.eventProcessor.impl.SerializedRequestHandler;
import io.pravega.controller.store.stream.ScaleOperationExceptions;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.DeleteStreamEvent;
import io.pravega.shared.controller.event.RequestProcessor;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import static io.pravega.controller.eventProcessor.impl.EventProcessorHelper.withRetries;

@Slf4j
public class StreamRequestHandler extends SerializedRequestHandler<ControllerEvent> implements RequestProcessor {
    private static final Predicate<Throwable> OPERATION_NOT_ALLOWED_PREDICATE = e -> e instanceof StoreException.OperationNotAllowedException;
    private final AutoScaleTask autoScaleTask;
    private final CommitTransactionTask commitTransactionTask;
    private final ScaleOperationTask scaleOperationTask;
    private final UpdateStreamTask updateStreamTask;
    private final SealStreamTask sealStreamTask;
    private final DeleteStreamTask deleteStreamTask;
    private final TruncateStreamTask truncateStreamTask;

    public StreamRequestHandler(AutoScaleTask autoScaleTask,
                                ScaleOperationTask scaleOperationTask,
                                CommitTransactionTask commitTransactionTask,
                                UpdateStreamTask updateStreamTask,
                                SealStreamTask sealStreamTask,
                                DeleteStreamTask deleteStreamTask,
                                TruncateStreamTask truncateStreamTask,
                                ScheduledExecutorService executor) {
        super(executor);
        this.autoScaleTask = autoScaleTask;
        this.commitTransactionTask = commitTransactionTask;
        this.scaleOperationTask = scaleOperationTask;
        this.updateStreamTask = updateStreamTask;
        this.sealStreamTask = sealStreamTask;
        this.deleteStreamTask = deleteStreamTask;
        this.truncateStreamTask = truncateStreamTask;
    }

    @Override
    public CompletableFuture<Void> processEvent(ControllerEvent controllerEvent) {
        return controllerEvent.process(this);
    }

    @Override
    public boolean toPostpone(ControllerEvent event, long pickupTime, Throwable exception) {
        // We will let the event be postponed for 2 minutes before declaring failure.
        return Exceptions.unwrap(exception) instanceof TaskExceptions.StartException &&
                (System.currentTimeMillis() - pickupTime) < Duration.ofMinutes(2).toMillis();
    }

    @Override
    public CompletableFuture<Void> processAbortTxnRequest(AbortEvent abortEvent) {
        return Futures.failedFuture(new RequestUnsupportedException(
                "StreamRequestHandler: abort txn received on Stream Request Multiplexer"));
    }

    @Override
    public CompletableFuture<Void> processCommitTxnRequest(CommitEvent commitEvent) {
        return withCompletion(commitTransactionTask, commitEvent,
                OPERATION_NOT_ALLOWED_PREDICATE.or(e -> e instanceof ScaleOperationExceptions.ScaleConflictException));
    }

    @Override
    public CompletableFuture<Void> processAutoScaleRequest(AutoScaleEvent autoScaleEvent) {
        return autoScaleTask.execute(autoScaleEvent);
    }

    @Override
    public CompletableFuture<Void> processScaleOpRequest(ScaleOpEvent scaleOpEvent) {
        return withCompletion(scaleOperationTask, scaleOpEvent, OPERATION_NOT_ALLOWED_PREDICATE);
    }

    @Override
    public CompletableFuture<Void> processUpdateStream(UpdateStreamEvent updateStreamEvent) {
        return withCompletion(updateStreamTask, updateStreamEvent, OPERATION_NOT_ALLOWED_PREDICATE);
    }

    @Override
    public CompletableFuture<Void> processTruncateStream(TruncateStreamEvent truncateStreamEvent) {
        return withCompletion(truncateStreamTask, truncateStreamEvent, OPERATION_NOT_ALLOWED_PREDICATE);
    }

    @Override
    public CompletableFuture<Void> processSealStream(SealStreamEvent sealStreamEvent) {
        return withCompletion(sealStreamTask, sealStreamEvent, OPERATION_NOT_ALLOWED_PREDICATE);
    }

    @Override
    public CompletableFuture<Void> processDeleteStream(DeleteStreamEvent deleteStreamEvent) {
        return withCompletion(deleteStreamTask, deleteStreamEvent, OPERATION_NOT_ALLOWED_PREDICATE);
    }

    private <T extends ControllerEvent> CompletableFuture<Void> withCompletion(StreamTask<T> task,
                                                                               T event,
                                                                               Predicate<Throwable> writeBackPredicate) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        withRetries(() -> task.execute(event), executor)
                .whenCompleteAsync((r, e) -> {
                    if (e != null) {
                        Throwable cause = Exceptions.unwrap(e);
                        if (writeBackPredicate.test(cause)) {
                            Retry.indefinitelyWithExpBackoff("Error writing event back into requeststream")
                                    .runAsync(() -> task.writeBack(event), executor)
                                    .thenAccept(v -> result.completeExceptionally(cause));
                        } else {
                            result.completeExceptionally(cause);
                        }
                    } else {
                        result.complete(r);
                    }
                }, executor);

        return result;
    }
}
