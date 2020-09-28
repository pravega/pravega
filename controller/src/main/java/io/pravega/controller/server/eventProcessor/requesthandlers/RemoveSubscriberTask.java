/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.records.StreamSubscribersRecord;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.RemoveSubscriberEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Request handler for performing scale operations received from requeststream.
 */
@Slf4j
public class RemoveSubscriberTask implements StreamTask<RemoveSubscriberEvent> {

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;

    public RemoveSubscriberTask(final StreamMetadataTasks streamMetadataTasks,
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
    public CompletableFuture<Void> execute(final RemoveSubscriberEvent request) {
        final OperationContext context = streamMetadataStore.createContext(request.getScope(), request.getStream());

        String scope = request.getScope();
        String stream = request.getStream();

        return streamMetadataStore.getVersionedState(scope, stream, context, executor)
                .thenCompose(versionedState -> streamMetadataStore.getSubscribersRecord(scope, stream, context, executor)
                        .thenCompose(versionedMetadata -> {
                            if (!versionedMetadata.getObject().isUpdating()) {
                                if (versionedState.getObject().equals(State.UPDATING_SUBSCRIBERS)) {
                                    return Futures.toVoid(streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE,
                                            versionedState, context, executor));
                                } else {
                                    return CompletableFuture.completedFuture(null);
                                }
                            } else {
                                return processUpdate(scope, stream, versionedMetadata, versionedState, context);
                            }
                        }));
    }

    private CompletableFuture<Void> processUpdate(String scope, String stream, VersionedMetadata<StreamSubscribersRecord> record,
                                                  VersionedMetadata<State> state, OperationContext context) {
        return Futures.toVoid(streamMetadataStore.updateVersionedState(scope, stream, State.UPDATING_SUBSCRIBERS, state, context, executor)
                      .thenCompose(updatedState -> streamMetadataStore.completeUpdateSubscribers(scope, stream, record, context, executor)
                      .thenCompose(x -> streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE, updatedState, context, executor))));
    }

    @Override
    public CompletableFuture<Void> writeBack(RemoveSubscriberEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }

    @Override
    public CompletableFuture<Boolean> hasTaskStarted(RemoveSubscriberEvent event) {
        return streamMetadataStore.getState(event.getScope(), event.getStream(), true, null, executor)
                                  .thenApply(state -> state.equals(State.UPDATING_SUBSCRIBERS));
    }
}
