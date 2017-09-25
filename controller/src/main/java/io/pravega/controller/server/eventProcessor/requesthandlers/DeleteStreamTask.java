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
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.DeleteStreamEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

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
        CompletableFuture<Void> result = new CompletableFuture<>();
        final OperationContext context = streamMetadataStore.createContext(request.getScope(), request.getStream());

        // TODO: implement delete stream task (issue 1738)
        // 1. check precondition
        // 2. set state deleting
        // 3. delete
        return result;
    }

    @Override
    public CompletableFuture<Void> writeBack(DeleteStreamEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }
}
