/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.store.stream.EpochTransitionOperationExceptions;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.RecordHelper;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.ScaleOpEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.LoggerFactory;

/**
 * Request handler for performing scale operations received from requeststream.
 */
public class ScaleOperationTask implements StreamTask<ScaleOpEvent> {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(ScaleOperationTask.class));

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;

    public ScaleOperationTask(final StreamMetadataTasks streamMetadataTasks,
                              final StreamMetadataStore streamMetadataStore,
                              final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetadataStore);
        Preconditions.checkNotNull(streamMetadataTasks);
        Preconditions.checkNotNull(executor);
        this.streamMetadataTasks = streamMetadataTasks;
        this.streamMetadataStore = streamMetadataStore;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> execute(final ScaleOpEvent request) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        final OperationContext context = streamMetadataStore.createContext(request.getScope(), request.getStream());

        log.info(request.getRequestId(), "starting scale request for {}/{} segments {} to new ranges {}",
                request.getScope(), request.getStream(), request.getSegmentsToSeal(), request.getNewRanges());

        runScale(request, request.isRunOnlyIfStarted(), context,
                this.streamMetadataTasks.retrieveDelegationToken())
                .whenCompleteAsync((res, e) -> {
                    if (e != null) {
                        Throwable cause = Exceptions.unwrap(e);
                        if (cause instanceof RetriesExhaustedException) {
                            cause = cause.getCause();
                        }
                        if (cause instanceof EpochTransitionOperationExceptions.PreConditionFailureException) {
                            log.warn(request.getRequestId(), "processing scale request for {}/{} segments {} failed {}",
                                    request.getScope(), request.getStream(), request.getSegmentsToSeal(), cause.getClass().getName());
                            result.complete(null);
                        } else {
                            log.warn(request.getRequestId(), "processing scale request for {}/{} segments {} failed {}",
                                    request.getScope(), request.getStream(), request.getSegmentsToSeal(), cause);
                            result.completeExceptionally(cause);
                        }
                    } else {
                        log.info(request.getRequestId(), "scale request for {}/{} segments {} to new ranges {} completed successfully.",
                                request.getScope(), request.getStream(), request.getSegmentsToSeal(), request.getNewRanges());

                        result.complete(null);
                    }
                }, executor);

        return result;
    }

    @Override
    public CompletableFuture<Void> writeBack(ScaleOpEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }

    @VisibleForTesting
    public CompletableFuture<Void> runScale(ScaleOpEvent scaleInput, boolean isManualScale, OperationContext context, String delegationToken) { // called upon event read from requeststream
        String scope = scaleInput.getScope();
        String stream = scaleInput.getStream();
        long requestId = scaleInput.getRequestId();
        // Note: There are two interesting cases for retry of partially completed workflow that are interesting:
        // if we crash before resetting epoch transition (complete scale step), then in rerun ETR will be rendered inconsistent
        // and deleted in startScale method.
        // if we crash before resetting state to active, in rerun (retry) we will find epoch transition to be empty and
        // hence reset the state here before attempting to start scale in idempotent fashion.
        return streamMetadataStore.getVersionedState(scope, stream, context, executor)
            .thenCompose(state -> streamMetadataStore.getEpochTransition(scope, stream, context, executor)
                    .thenCompose(record -> {
                        AtomicReference<VersionedMetadata<State>> reference = new AtomicReference<>(state);
                        CompletableFuture<VersionedMetadata<EpochTransitionRecord>> future = CompletableFuture.completedFuture(record);
                        if (record.getObject().equals(EpochTransitionRecord.EMPTY)) {
                            if (state.getObject().equals(State.SCALING)) {
                                future = streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE,
                                        state, context, executor).thenApply(updatedState -> {
                                            reference.set(updatedState);
                                            return record;
                                });
                            } 

                            if (isManualScale) {
                                log.info("Found empty epoch transition record, scale processing is already completed.");
                                return Futures.toVoid(future);
                            } else {
                                future = future.thenCompose(r -> streamMetadataStore.submitScale(scope, stream, scaleInput.getSegmentsToSeal(),
                                        new ArrayList<>(scaleInput.getNewRanges()), scaleInput.getScaleTime(), record, context, executor));
                            }
                        } else if (!RecordHelper.verifyRecordMatchesInput(scaleInput.getSegmentsToSeal(), scaleInput.getNewRanges(), isManualScale, record.getObject())) {
                            // ensure that we process the event only if the input matches the epoch transition record.
                            // if its manual scale and the event doesnt match, it means the scale has already completed. 
                            // for auto scale, it means another scale is on going and just throw scale conflict exception 
                            // which will result in this event being postponed. 
                            if (isManualScale) {
                                log.info("Scale for stream {}/{} for segments {} already completed.", scaleInput.getScope(), scaleInput.getStream(), scaleInput.getSegmentsToSeal());
                                return CompletableFuture.completedFuture(null);
                            } else {
                                log.info("Scale for stream {}/{} for segments {} cannot be started as another scale is ongoing.", scaleInput.getScope(), scaleInput.getStream(), scaleInput.getSegmentsToSeal());
                                throw new EpochTransitionOperationExceptions.ConflictException();
                            }
                        }
                        
                        return future
                                .thenCompose(versionedMetadata -> processScale(scope, stream, isManualScale, versionedMetadata, 
                                        reference.get(), context, delegationToken, requestId));
                    }));
    }
    
    private CompletableFuture<Void> processScale(String scope, String stream, boolean isManualScale,
                                                 VersionedMetadata<EpochTransitionRecord> metadata,
                                                 VersionedMetadata<State> state, OperationContext context,
                                                 String delegationToken, long requestId) {
        return streamMetadataStore.updateVersionedState(scope, stream, State.SCALING, state, context, executor)
                .thenCompose(updatedState -> streamMetadataStore.startScale(scope, stream, isManualScale, metadata, updatedState, context, executor)
                        .thenCompose(record -> {
                            List<Long> segmentIds = new ArrayList<>(record.getObject().getNewSegmentsWithRange().keySet());
                            List<Long> segmentsToSeal = new ArrayList<>(record.getObject().getSegmentsToSeal());
                            return streamMetadataTasks.notifyNewSegments(scope, stream, segmentIds, context, delegationToken, requestId)
                                    .thenCompose(x -> streamMetadataStore.scaleCreateNewEpochs(scope, stream, record, context, executor))
                                    .thenCompose(x -> streamMetadataTasks.notifySealedSegments(scope, stream, segmentsToSeal, delegationToken, requestId))
                                    .thenCompose(x -> streamMetadataTasks.getSealedSegmentsSize(scope, stream, segmentsToSeal, delegationToken))
                                    .thenCompose(map -> streamMetadataStore.scaleSegmentsSealed(scope, stream, map, record, context, executor))
                                    .thenCompose(x -> streamMetadataStore.completeScale(scope, stream, record, context, executor))
                                    .thenCompose(x -> streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE, updatedState, context, executor))
                                    .thenAccept(y -> {
                                        log.info(requestId, "scale processing for {}/{} epoch {} completed.", scope, stream, record.getObject().getActiveEpoch());
                                    });
                        }));

    }

    @Override
    public CompletableFuture<Boolean> hasTaskStarted(ScaleOpEvent event) {
        return streamMetadataStore.getState(event.getScope(), event.getStream(), true, null, executor)
                                  .thenApply(state -> state.equals(State.SCALING));
    }
}
