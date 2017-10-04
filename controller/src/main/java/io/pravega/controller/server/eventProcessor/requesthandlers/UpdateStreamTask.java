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
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamConfigWithVersion;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Request handler for performing scale operations received from requeststream.
 */
@Slf4j
public class UpdateStreamTask implements StreamTask<UpdateStreamEvent> {

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;

    public UpdateStreamTask(final StreamMetadataTasks streamMetadataTasks,
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
    public CompletableFuture<Void> execute(final UpdateStreamEvent request) {
        final OperationContext context = streamMetadataStore.createContext(request.getScope(), request.getStream());

        // when update stream event is picked, update task is performed.
        // 1. check state is updating. else postpone [predicate(time, numberOfAttempts)]
        // 2. get configuration. If configuration.version == input.version -> perform update
        // else idempotent check: configuration.version = input.version + 1 && configuration.config == input.configuration.
        // 3. notify segment store about updated configuration
        // 4. set state to active.

        String scope = request.getScope();
        String stream = request.getStream();
        StreamConfiguration newConfig = ModelHelper.encode(request.getStreamConfig());

        return streamMetadataStore.getState(scope, stream, true, context, executor)
                .thenAccept(state -> {
                    if (!state.equals(State.UPDATING)) {
                        throw new TaskExceptions.StartException();
                    }
                })
                .thenCompose(x ->
                        streamMetadataStore.getConfigurationWithVersion(scope, stream, context, executor)
                                .thenCompose(configWithVersion -> {
                                    if (request.getVersion() == configWithVersion.getVersion()) {
                                        return streamMetadataStore.updateConfiguration(scope, stream,
                                                StreamConfigWithVersion.generateNext(configWithVersion, newConfig),
                                                context, executor);
                                    } else {
                                        // idempotent
                                        if (configWithVersion.getVersion() == request.getVersion() + 1 &&
                                                configWithVersion.getConfiguration().equals(newConfig)) {
                                            return CompletableFuture.completedFuture(true);
                                        } else {
                                            return CompletableFuture.completedFuture(false);
                                        }
                                    }
                                }))
                .thenCompose(updated -> {
                    if (updated) {
                        log.debug("{}/{} updated in metadata store", scope, stream);

                        // we are at a point of no return. Metadata has been updated, we need to notify hosts.
                        // wrap subsequent steps in retries.
                        return streamMetadataStore.getActiveSegments(scope, stream, context, executor)
                                .thenCompose(activeSegments -> streamMetadataTasks.notifyPolicyUpdates(scope, stream, activeSegments, newConfig.getScalingPolicy()))
                                .handle((res, ex) -> {
                                    if (ex == null) {
                                        return true;
                                    } else {
                                        throw new CompletionException(ex);
                                    }
                                });
                    } else {
                        return CompletableFuture.completedFuture(false);
                    }
                }).thenCompose(x -> FutureHelpers.toVoid(streamMetadataStore.setState(scope, stream, State.ACTIVE, context, executor)));
    }

    @Override
    public CompletableFuture<Void> writeBack(UpdateStreamEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }
}
