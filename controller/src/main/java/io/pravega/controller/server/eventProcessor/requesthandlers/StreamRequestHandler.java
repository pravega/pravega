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

import io.pravega.controller.store.stream.EpochTransitionOperationExceptions;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.DeleteStreamEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import io.pravega.shared.controller.event.AddSubscriberEvent;
import io.pravega.shared.controller.event.RemoveSubscriberEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class StreamRequestHandler extends AbstractRequestProcessor<ControllerEvent> {
    private final AutoScaleTask autoScaleTask;
    private final ScaleOperationTask scaleOperationTask;
    private final UpdateStreamTask updateStreamTask;
    private final SealStreamTask sealStreamTask;
    private final DeleteStreamTask deleteStreamTask;
    private final TruncateStreamTask truncateStreamTask;
    private final AddSubscriberTask addSubscriberTask;
    private final RemoveSubscriberTask removeSubscriberTask;

    public StreamRequestHandler(AutoScaleTask autoScaleTask,
                                ScaleOperationTask scaleOperationTask,
                                UpdateStreamTask updateStreamTask,
                                SealStreamTask sealStreamTask,
                                DeleteStreamTask deleteStreamTask,
                                TruncateStreamTask truncateStreamTask,
                                AddSubscriberTask addSubscriberTask,
                                RemoveSubscriberTask removeSubscriberTask,
                                StreamMetadataStore streamMetadataStore,
                                ScheduledExecutorService executor) {
        super(streamMetadataStore, executor);
        this.autoScaleTask = autoScaleTask;
        this.scaleOperationTask = scaleOperationTask;
        this.updateStreamTask = updateStreamTask;
        this.sealStreamTask = sealStreamTask;
        this.deleteStreamTask = deleteStreamTask;
        this.truncateStreamTask = truncateStreamTask;
        this.addSubscriberTask = addSubscriberTask;
        this.removeSubscriberTask = removeSubscriberTask;
    }
    
    @Override
    public CompletableFuture<Void> processAutoScaleRequest(AutoScaleEvent autoScaleEvent) {
        return autoScaleTask.execute(autoScaleEvent);
    }

    @Override
    public CompletableFuture<Void> processScaleOpRequest(ScaleOpEvent scaleOpEvent) {
        log.info("Processing scale request {} for stream {}/{}", scaleOpEvent.getRequestId(), scaleOpEvent.getScope(), scaleOpEvent.getStream());
        return withCompletion(scaleOperationTask, scaleOpEvent, scaleOpEvent.getScope(), scaleOpEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE.or(e -> e instanceof EpochTransitionOperationExceptions.ConflictException))
                .thenAccept(v -> {
                    log.info("Processing scale request {} for stream {}/{} complete", scaleOpEvent.getRequestId(), scaleOpEvent.getScope(), scaleOpEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processUpdateStream(UpdateStreamEvent updateStreamEvent) {
        log.info("Processing update request {} for stream {}/{}", updateStreamEvent.getRequestId(), updateStreamEvent.getScope(), updateStreamEvent.getStream());
        return withCompletion(updateStreamTask, updateStreamEvent, updateStreamEvent.getScope(), updateStreamEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info("Processing update request {} for stream {}/{} complete", updateStreamEvent.getRequestId(), updateStreamEvent.getScope(), updateStreamEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processAddSubscriberStream(AddSubscriberEvent addSubscriberEvent) {
        log.info("Processing update request {} for stream {}/{}", addSubscriberEvent.getRequestId(), addSubscriberEvent.getScope(), addSubscriberEvent.getStream());
        return withCompletion(addSubscriberTask, addSubscriberEvent, addSubscriberEvent.getScope(), addSubscriberEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info("Processing add subscriber request {} for stream {}/{} complete", addSubscriberEvent.getRequestId(),
                                                                    addSubscriberEvent.getScope(), addSubscriberEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processRemoveSubscriberStream(RemoveSubscriberEvent removeSubscriberEvent) {
        log.info("Processing update request {} for stream {}/{}", removeSubscriberEvent.getRequestId(), removeSubscriberEvent.getScope(), removeSubscriberEvent.getStream());
        return withCompletion(removeSubscriberTask, removeSubscriberEvent, removeSubscriberEvent.getScope(), removeSubscriberEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info("Processing add subscriber request {} for stream {}/{} complete", removeSubscriberEvent.getRequestId(),
                            removeSubscriberEvent.getScope(), removeSubscriberEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processTruncateStream(TruncateStreamEvent truncateStreamEvent) {
        log.info("Processing truncate request {} for stream {}/{}", truncateStreamEvent.getRequestId(), truncateStreamEvent.getScope(), truncateStreamEvent.getStream());
        return withCompletion(truncateStreamTask, truncateStreamEvent, truncateStreamEvent.getScope(), truncateStreamEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info("Processing truncate request {} for stream {}/{} complete", truncateStreamEvent.getRequestId(), truncateStreamEvent.getScope(), truncateStreamEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processSealStream(SealStreamEvent sealStreamEvent) {
        log.info("Processing seal request {} for stream {}/{}", sealStreamEvent.getRequestId(), sealStreamEvent.getScope(), sealStreamEvent.getStream());
        return withCompletion(sealStreamTask, sealStreamEvent, sealStreamEvent.getScope(), sealStreamEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info("Processing seal request {} for stream {}/{} complete", sealStreamEvent.getRequestId(), sealStreamEvent.getScope(), sealStreamEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processDeleteStream(DeleteStreamEvent deleteStreamEvent) {
        log.info("Processing delete request {} for stream {}/{}", deleteStreamEvent.getRequestId(), deleteStreamEvent.getScope(), deleteStreamEvent.getStream());
        return withCompletion(deleteStreamTask, deleteStreamEvent, deleteStreamEvent.getScope(), deleteStreamEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info("Processing delete request {} for stream {}/{} complete", deleteStreamEvent.getRequestId(), deleteStreamEvent.getScope(), deleteStreamEvent.getStream());
                });
    }
}
