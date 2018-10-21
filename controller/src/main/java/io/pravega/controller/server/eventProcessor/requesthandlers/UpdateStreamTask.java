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
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StreamConfigurationRecord;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;

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

        String scope = request.getScope();
        String stream = request.getStream();

        return streamMetadataStore.getVersionedState(scope, stream, context, executor)
                .thenCompose(versionedState -> streamMetadataStore.getConfigurationRecord(scope, stream, context, executor)
                        .thenCompose(versionedMetadata -> {
                            if (!versionedMetadata.getObject().isUpdating()) {
                                if (versionedState.getObject().equals(State.UPDATING)) {
                                    return Futures.toVoid(streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE,
                                            versionedState, context, executor));
                                } else {
                                    throw new TaskExceptions.StartException("Update Stream not started yet.");
                                }
                            } else {
                                return processUpdate(scope, stream, versionedMetadata, versionedState, context);
                            }
                        }));
    }

    private CompletableFuture<Void> processUpdate(String scope, String stream, VersionedMetadata<StreamConfigurationRecord> record,
                                                  VersionedMetadata<State> state, OperationContext context) {
        StreamConfigurationRecord configProperty = record.getObject();

        return Futures.toVoid(streamMetadataStore.updateVersionedState(scope, stream, State.UPDATING, state, context, executor)
                .thenCompose(updated -> updateStreamForAutoStreamCut(scope, stream, context, configProperty, updated)
                        .thenCompose(x -> notifyPolicyUpdate(context, scope, stream, configProperty.getStreamConfiguration()))
                        .thenCompose(x -> streamMetadataStore.completeUpdateConfiguration(scope, stream, record, context, executor))
                        .thenCompose(x -> streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE, updated, context, executor))));
    }

    private CompletableFuture<Void> updateStreamForAutoStreamCut(String scope, String stream,
                        OperationContext context, StreamConfigurationRecord configProperty, VersionedMetadata<State> updated) {
        if (configProperty.getStreamConfiguration().getRetentionPolicy() != null) {
            return streamMetadataStore.addUpdateStreamForAutoStreamCut(scope, stream,
                    configProperty.getStreamConfiguration().getRetentionPolicy(), context, executor);
        } else {
            return streamMetadataStore.removeStreamFromAutoStreamCut(scope, stream, context, executor);
        }
    }

    private CompletableFuture<Boolean> notifyPolicyUpdate(OperationContext context, String scope, String stream, StreamConfiguration newConfig) {
        return streamMetadataStore.getActiveSegments(scope, stream, context, executor)
                .thenCompose(activeSegments -> streamMetadataTasks.notifyPolicyUpdates(scope, stream, activeSegments, newConfig.getScalingPolicy(),
                        this.streamMetadataTasks.retrieveDelegationToken()))
                .handle((res, ex) -> {
                    if (ex == null) {
                        return true;
                    } else {
                        throw new CompletionException(ex);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> writeBack(UpdateStreamEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }
}
