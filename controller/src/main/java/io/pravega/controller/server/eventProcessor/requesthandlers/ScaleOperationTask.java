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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.ScaleOpEvent;
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

        streamMetadataTasks.startScale(request, request.isRunOnlyIfStarted(), context,
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
}
