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
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.ScaleOpEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import lombok.extern.slf4j.Slf4j;

/**
 * Request handler for performing scale operations received from requeststream.
 */
@Slf4j
public class ScaleOperationTask implements StreamTask<ScaleOpEvent> {

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

        log.info("starting scale request for {}/{} segments {} to new ranges {}", request.getScope(), request.getStream(),
                request.getSegmentsToSeal(), request.getNewRanges());

        runScale(request, request.isRunOnlyIfStarted(), context,
                this.streamMetadataTasks.retrieveDelegationToken())
                .whenCompleteAsync((res, e) -> {
                    if (e != null) {
                        Throwable cause = Exceptions.unwrap(e);
                        if (cause instanceof RetriesExhaustedException) {
                            cause = cause.getCause();
                        }
                        log.warn("processing scale request for {}/{} segments {} failed {}", request.getScope(), request.getStream(),
                                request.getSegmentsToSeal(), cause);
                        result.completeExceptionally(cause);
                    } else {
                        log.info("scale request for {}/{} segments {} to new ranges {} completed successfully.", request.getScope(), request.getStream(),
                                request.getSegmentsToSeal(), request.getNewRanges());

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
        // Note: There are two interesting cases for retry of partially completed workflow that are interesting:
        // if we crash before resetting epoch transition (complete scale step), then in rerun ETR will be rendered inconsistent
        // and deleted in startScale method.
        // if we crash before resetting state to active, in rerun (retry) we will find epoch transition to be empty and
        // hence reset the state here before attempting to start scale in idempotent fashion.
        return streamMetadataStore.getVersionedState(scope, stream, context, executor)
            .thenCompose(state -> streamMetadataStore.getEpochTransition(scope, stream, context, executor)
                    .thenCompose(record -> {
                        CompletableFuture<VersionedMetadata<EpochTransitionRecord>> future = CompletableFuture.completedFuture(record);
                        if (record.getObject().equals(EpochTransitionRecord.EMPTY)) {
                            if (state.getObject().equals(State.SCALING)) {
                                return Futures.toVoid(streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE,
                                        state, context, executor));
                            } else {
                                if (isManualScale) {
                                    throw new TaskExceptions.StartException("Scale Stream not started yet.");
                                } else {
                                    future = streamMetadataStore.submitScale(scope, stream, scaleInput.getSegmentsToSeal(),
                                            scaleInput.getNewRanges(), scaleInput.getScaleTime(), context, executor);
                                }
                            }
                        } 
                        
                        return future
                                .thenCompose(versionedMetadata -> processScale(scope, stream, isManualScale, delegationToken,
                                        versionedMetadata, state, context));
                    }));
    }

    private CompletableFuture<Void> processScale(String scope, String stream, boolean isManualScale, String delegationToken,
                                                                  VersionedMetadata<EpochTransitionRecord> metadata,
                                                                  VersionedMetadata<State> state, OperationContext context) {
        return streamMetadataStore.updateVersionedState(scope, stream, State.SCALING, state, context, executor)
                .thenCompose(updatedState -> streamMetadataStore.startScale(scope, stream, isManualScale, metadata, updatedState, context, executor)
                        .thenCompose(record -> streamMetadataStore.scaleCreateNewSegments(scope, stream, record, context, executor)
                        .thenCompose(v -> {
                            List<Long> segmentIds = record.getObject().getNewSegmentsWithRange().keySet().asList();
                            List<Long> segmentsToSeal = record.getObject().getSegmentsToSeal().asList();
                            return streamMetadataTasks.notifyNewSegments(scope, stream, segmentIds, context, delegationToken)
                                    .thenCompose(x -> streamMetadataStore.scaleNewSegmentsCreated(scope, stream, record, context, executor))
                                    .thenCompose(x -> streamMetadataTasks.notifySealedSegments(scope, stream, segmentsToSeal, delegationToken))
                                    .thenCompose(x -> streamMetadataTasks.getSealedSegmentsSize(scope, stream, segmentsToSeal, delegationToken))
                                    .thenCompose(map -> streamMetadataStore.scaleSegmentsSealed(scope, stream, map, record, context, executor))
                                    .thenCompose(x -> streamMetadataStore.completeScale(scope, stream, record, context, executor))
                                    .thenCompose(x -> streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE, updatedState, context, executor))
                                    .thenAccept(y -> {
                                        log.info("scale processing for {}/{} epoch {} completed.", scope, stream, record.getObject().getActiveEpoch());
                                    });
                        })));

    }
}
