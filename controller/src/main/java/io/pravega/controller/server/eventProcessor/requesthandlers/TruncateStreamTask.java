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
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamProperty;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Request handler for performing scale operations received from requeststream.
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
                        return processTruncate(scope, stream, property, context);
                    }
                });
    }

    private CompletableFuture<Void> processTruncate(String scope, String stream, StreamProperty<StreamTruncationRecord> streamCut,
                                                    OperationContext context) {
        // TODO: shivesh
        // find segments to delete --> all predecessors till a certain point.
        //
//        streamMetadataStore.completeTruncation(scope, stream, Collections.emptySet(), context, executor);
        return null;
    }

    @Override
    public CompletableFuture<Void> writeBack(TruncateStreamEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }
}
