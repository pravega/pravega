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
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.DeleteStreamEvent;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

/**
 * Request handler for performing scale operations received from requeststream.
 */
@Slf4j
public class DeleteStreamTask implements StreamTask<DeleteStreamEvent> {

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;

    public DeleteStreamTask(final StreamMetadataTasks streamMetadataTasks,
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
    public CompletableFuture<Void> execute(final DeleteStreamEvent request) {
        final OperationContext context = streamMetadataStore.createContext(request.getScope(), request.getStream());

        String scope = request.getScope();
        String stream = request.getStream();
        long requestId = request.getRequestId();
        return streamMetadataStore.isSealed(scope, stream, context, executor)
                .thenComposeAsync(sealed -> {
                    if (!sealed) {
                        LoggerHelpers.warnLogWithTag(log, requestId, "{}/{} stream not sealed", scope, stream);
                        return Futures.failedFuture(new RuntimeException("Stream not sealed"));
                    }
                    return notifyAndDelete(context, scope, stream, requestId);
                }, executor)
                .exceptionally(e -> {
                    if (e instanceof StoreException.DataNotFoundException) {
                        return null;
                    }
                    LoggerHelpers.errorLogWithTag(log, requestId, "{}/{} stream delete workflow threw exception.", scope, stream, e);

                    throw new CompletionException(e);
                });
    }

    private CompletableFuture<Void> notifyAndDelete(OperationContext context, String scope, String stream, long requestId) {
        LoggerHelpers.infoLogWithTag(log, requestId, "{}/{} deleting segments", scope, stream);
        return streamMetadataStore.getScaleMetadata(scope, stream, context, executor)
                .thenComposeAsync(scaleMetadata -> {
                    Set<Long> toDelete = new HashSet<>();
                    scaleMetadata.forEach(x -> toDelete.addAll(x.getSegments().stream().map(Segment::segmentId).collect(Collectors.toList())));
                    return streamMetadataTasks.notifyDeleteSegments(scope, stream, toDelete, streamMetadataTasks.retrieveDelegationToken(), requestId)
                            .thenComposeAsync(x -> streamMetadataStore.removeStreamFromAutoStreamCut(scope, stream, context,
                                    executor), executor)
                            .thenComposeAsync(x -> streamMetadataStore.deleteStream(scope, stream, context,
                                    executor), executor);
                });
    }

    @Override
    public CompletableFuture<Void> writeBack(DeleteStreamEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }
}
