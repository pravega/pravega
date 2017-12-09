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
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.rpc.auth.PravegaInterceptor;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Request handler for performing truncation operations received from requeststream.
 */
@Slf4j
public class TruncateStreamTask implements StreamTask<TruncateStreamEvent> {

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;

    public TruncateStreamTask(final StreamMetadataTasks streamMetadataTasks,
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
    public CompletableFuture<Void> execute(final TruncateStreamEvent request) {
        final OperationContext context = streamMetadataStore.createContext(request.getScope(), request.getStream());

        String scope = request.getScope();
        String stream = request.getStream();

        return streamMetadataStore.getTruncationProperty(scope, stream, true, context, executor)
                .thenCompose(property -> {
                    if (!property.isUpdating()) {
                        throw new TaskExceptions.StartException("Truncate Stream not started yet.");
                    } else {
                        //TODO: Get proper token
                        return processTruncate(scope, stream, property.getProperty(), context, PravegaInterceptor.retrieveDelegationToken(""));
                    }
                });
    }

    private CompletableFuture<Void> processTruncate(String scope, String stream, StreamTruncationRecord truncationRecord,
                                                    OperationContext context, String delegationToken) {
        return Futures.toVoid(streamMetadataStore.setState(scope, stream, State.TRUNCATING, context, executor)
                 .thenCompose(x -> notifyTruncateSegments(scope, stream, truncationRecord.getStreamCut(), delegationToken))
                 .thenCompose(x -> notifyDeleteSegments(scope, stream, truncationRecord.getToDelete(), delegationToken))
                 .thenCompose(deleted -> streamMetadataStore.completeTruncation(scope, stream, context, executor))
                 .thenCompose(x -> streamMetadataStore.setState(scope, stream, State.ACTIVE, context, executor)));
    }

    private CompletableFuture<Void> notifyDeleteSegments(String scope, String stream, Set<Integer> segmentsToDelete, String delegationToken) {
        log.debug("{}/{} deleting segments {}", scope, stream, segmentsToDelete);
        return Futures.allOf(segmentsToDelete.stream()
                .parallel()
                .map(segment -> streamMetadataTasks.notifyDeleteSegment(scope, stream, segment, delegationToken))
                .collect(Collectors.toList()));
    }

    private CompletableFuture<Void> notifyTruncateSegments(String scope, String stream, Map<Integer, Long> streamCut, String delegationToken) {
        log.debug("{}/{} truncating segments", scope, stream);
        return Futures.allOf(streamCut.entrySet().stream()
                .parallel()
                .map(segmentCut -> streamMetadataTasks.notifyTruncateSegment(scope, stream, segmentCut, delegationToken))
                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> writeBack(TruncateStreamEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }
}
