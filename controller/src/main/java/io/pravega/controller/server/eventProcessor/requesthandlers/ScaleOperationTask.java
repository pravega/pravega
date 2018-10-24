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
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.ScaleOpEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

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
                        log.warn(request.getRequestId(), "processing scale request for {}/{} segments {} failed {}",
                                request.getScope(), request.getStream(), request.getSegmentsToSeal(), cause);
                        result.completeExceptionally(cause);
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
    public CompletableFuture<EpochTransitionRecord> runScale(ScaleOpEvent scaleInput, boolean runOnlyIfStarted, OperationContext context, String delegationToken) { // called upon event read from requeststream
        String scope = scaleInput.getScope();
        String stream = scaleInput.getStream();
        long requestId = scaleInput.getRequestId();
        // create epoch transition node (metadatastore.startScale)
        // Note: if we crash before deleting epoch transition, then in rerun (retry) it will be rendered inconsistent
        // and deleted in startScale method.
        // if we crash before setting state to active, in rerun (retry) we will find epoch transition to be null and
        // hence reset the state in startScale method before attempting to start scale in idempotent fashion.
        return streamMetadataStore.startScale(scope, stream, scaleInput.getSegmentsToSeal(), scaleInput.getNewRanges(),
                scaleInput.getScaleTime(), runOnlyIfStarted, context, executor)
                .thenCompose(response -> streamMetadataStore.setState(scope, stream, State.SCALING, context, executor)
                        .thenCompose(v -> streamMetadataStore.scaleCreateNewSegments(scope, stream, runOnlyIfStarted, context, executor))
                        .thenCompose(v -> {
                            List<Long> segmentIds = response.getNewSegmentsWithRange().keySet().asList();
                            return streamMetadataTasks.notifyNewSegments(scope, stream, segmentIds, context, delegationToken, requestId)
                                    .thenCompose(x -> streamMetadataStore.scaleNewSegmentsCreated(scope, stream, context, executor))
                                    .thenCompose(x -> streamMetadataTasks.notifySealedSegments(scope, stream, scaleInput.getSegmentsToSeal(), delegationToken, requestId))
                                    .thenCompose(x -> streamMetadataTasks.getSealedSegmentsSize(scope, stream, scaleInput.getSegmentsToSeal(), delegationToken))
                                    .thenCompose(map -> streamMetadataStore.scaleSegmentsSealed(scope, stream, map, context, executor))
                                    .thenCompose(x -> streamMetadataStore.setState(scope, stream, State.ACTIVE, context, executor))
                                    .thenApply(y -> {
                                        log.info(requestId, "scale processing for {}/{} epoch {} completed.",
                                                scope, stream, response.getActiveEpoch());
                                        return response;
                                    });
                        }));
    }

}
