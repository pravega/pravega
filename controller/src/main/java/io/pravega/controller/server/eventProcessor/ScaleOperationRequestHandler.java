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

import com.google.common.base.Preconditions;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Request handler for performing scale operations received from requeststream.
 */
@Slf4j
public class ScaleOperationRequestHandler implements RequestHandler<ScaleOpEvent> {

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;

    public ScaleOperationRequestHandler(final StreamMetadataTasks streamMetadataTasks,
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
    public CompletableFuture<Void> process(final ScaleOpEvent request, final EventProcessor.Writer<ScaleOpEvent> writer) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        final OperationContext context = streamMetadataStore.createContext(request.getScope(), request.getStream());

        log.info("starting scale request for {}/{} segments {} to new ranges {}", request.getScope(), request.getStream(),
                request.getSegmentsToSeal(), request.getNewRanges());

        streamMetadataTasks.startScale(request, request.isRunOnlyIfStarted(), context)
                .whenCompleteAsync((res, e) -> {
                    if (e != null) {
                        Throwable cause = ExceptionHelpers.getRealException(e);
                        if (cause instanceof RetriesExhaustedException) {
                            cause = cause.getCause();
                        }
                        result.completeExceptionally(cause);
                    } else {
                        result.complete(null);
                    }
                }, executor);

        return result;
    }
}
