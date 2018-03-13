/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.SealStreamEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Request handler for performing scale operations received from requeststream.
 */
@Slf4j
public class SealStreamTask implements StreamTask<SealStreamEvent> {

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;

    public SealStreamTask(final StreamMetadataTasks streamMetadataTasks,
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
    public CompletableFuture<Void> execute(final SealStreamEvent request) {
        String scope = request.getScope();
        String stream = request.getStream();
        final OperationContext context = streamMetadataStore.createContext(scope, stream);

        // when seal stream task is picked, if the state is sealing/sealed, process sealing, else postpone.
        return streamMetadataStore.getState(scope, stream, true, context, executor)
                .thenAccept(state -> {
                    if (!state.equals(State.SEALING) && !state.equals(State.SEALED)) {
                        throw new TaskExceptions.StartException("Seal stream task not started yet.");
                    }
                })
                .thenCompose(x -> streamMetadataStore.getActiveSegments(scope, stream, context, executor))
                .thenCompose(activeSegments -> {
                    if (activeSegments.isEmpty()) {
                        // idempotent check
                        // if active segment set is empty then the stream is sealed.
                        // Do not update the state if the stream is already sealed.
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return notifySealed(scope, stream, context, activeSegments);
                    }
                });
    }

    private CompletionStage<Void> notifySealed(String scope, String stream, OperationContext context, List<Segment> activeSegments) {
        List<Integer> segmentsToBeSealed = activeSegments.stream().map(Segment::getNumber).
                collect(Collectors.toList());
        log.info("Sending notification to segment store to seal segments for stream {}/{}", scope, stream);
        return streamMetadataTasks.notifySealedSegments(scope, stream, segmentsToBeSealed)
                .thenCompose(v -> setSealed(scope, stream, context));
    }

    private CompletableFuture<Void> setSealed(String scope, String stream, OperationContext context) {
        return Futures.toVoid(streamMetadataStore.setSealed(scope, stream, context, executor));
    }

    @Override
    public CompletableFuture<Void> writeBack(SealStreamEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }
}
