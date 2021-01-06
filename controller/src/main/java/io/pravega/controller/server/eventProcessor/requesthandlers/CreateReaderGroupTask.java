/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers;

import com.google.common.base.Preconditions;

import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.common.Exceptions;

import io.pravega.common.concurrent.Futures;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.RGOperationContext;
import io.pravega.controller.store.stream.ReaderGroupState;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;

import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.CreateReaderGroupEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * Request handler for executing a create operation for a ReaderGroup.
 */
@Slf4j
public class CreateReaderGroupTask implements ReaderGroupTask<CreateReaderGroupEvent> {

    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executor;

    public CreateReaderGroupTask(final StreamMetadataTasks streamMetaTasks,
                                 final StreamMetadataStore streamMetaStore,
                                 final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetaStore);
        Preconditions.checkNotNull(streamMetaTasks);
        Preconditions.checkNotNull(executor);
        this.streamMetadataStore = streamMetaStore;
        this.streamMetadataTasks = streamMetaTasks;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> execute(final CreateReaderGroupEvent request) {
        String scope = request.getScope();
        String readerGroup = request.getRgName();
        UUID readerGroupId = request.getReaderGroupId();
        final RGOperationContext context = streamMetadataStore.createRGContext(scope, readerGroup);
        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.getReaderGroupId(scope, readerGroup, context, executor)
                .thenCompose(rgId -> {
                    if (!rgId.equals(readerGroupId)) {
                        log.warn("Skipping processing of CreateReaderGroupEvent with invalid UUID.");
                        return CompletableFuture.completedFuture(null);
                    }
                    ReaderGroupConfig config = getConfigFromEvent(request);
                    return Futures.exceptionallyExpecting(streamMetadataStore.getReaderGroupState(scope, readerGroup, true, null, executor),
                            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, ReaderGroupState.UNKNOWN)
                            .thenCompose(state -> {
                                if (state.equals(ReaderGroupState.UNKNOWN) || state.equals(ReaderGroupState.CREATING)) {
                                    return streamMetadataStore.createReaderGroup(scope, readerGroup, config, System.currentTimeMillis(), context, executor)
                                            .thenCompose(v -> {
                                             if (!ReaderGroupConfig.StreamDataRetention.NONE.equals(config.getRetentionType())) {
                                                    String scopedRGName = NameUtils.getScopedReaderGroupName(scope, readerGroup);
                                                    // update Stream metadata tables, only if RG is a Subscriber
                                                    Iterator<String> streamIter = config.getStartingStreamCuts().keySet().stream()
                                                            .map(s -> s.getScopedName()).iterator();
                                                    return Futures.loop(() -> streamIter.hasNext(), () -> {
                                                        Stream stream = Stream.of(streamIter.next());
                                                        return streamMetadataStore.addSubscriber(stream.getScope(),
                                                                stream.getStreamName(), scopedRGName, config.getGeneration(), null, executor);
                                                    }, executor);
                                                }
                                                return CompletableFuture.completedFuture(null);
                                            }).thenCompose(x -> streamMetadataTasks.createRGStream(scope, NameUtils.getStreamForReaderGroup(readerGroup),
                                                    StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(),
                                                    System.currentTimeMillis(), 10)
                                                    .thenCompose(createStatus -> {
                                                        if (createStatus.equals(Controller.CreateStreamStatus.Status.STREAM_EXISTS)
                                                                || createStatus.equals(Controller.CreateStreamStatus.Status.SUCCESS)) {
                                                            return Futures.toVoid(streamMetadataStore.getVersionedReaderGroupState(scope, readerGroup, true, null, executor)
                                                                    .thenCompose(newstate -> streamMetadataStore.updateReaderGroupVersionedState(scope, readerGroup,
                                                                    ReaderGroupState.ACTIVE, newstate, context, executor)));
                                                        }
                                                        return Futures.failedFuture(new IllegalStateException(String.format("Error creating StateSynchronizer Stream for Reader Group %s: %s",
                                                                readerGroup, createStatus.toString())));
                                                    })).exceptionally(ex -> {
                                                log.debug(ex.getMessage());
                                                Throwable cause = Exceptions.unwrap(ex);
                                                throw new CompletionException(cause);
                                            });
                                }
                                return CompletableFuture.completedFuture(null);
                            });
        }), e -> Exceptions.unwrap(e) instanceof RetryableException, Integer.MAX_VALUE, executor);
    }

    private ReaderGroupConfig getConfigFromEvent(CreateReaderGroupEvent request) {
        Map<Stream, StreamCut> startStreamCut = request.getStartingStreamCuts().entrySet()
                                                .stream().collect(Collectors.toMap(e -> Stream.of(e.getKey()),
                                                e -> new StreamCutImpl(Stream.of(e.getKey()),
                                                        ModelHelper.getSegmentOffsetMap(Stream.of(e.getKey()).getScope(),
                                                                Stream.of(e.getKey()).getStreamName(),
                                                                e.getValue().getStreamCut()))));
        Map<Stream, StreamCut> endStreamCut = request.getEndingStreamCuts().entrySet()
                .stream().collect(Collectors.toMap(e -> Stream.of(e.getKey()),
                        e -> new StreamCutImpl(Stream.of(e.getKey()),
                                ModelHelper.getSegmentOffsetMap(Stream.of(e.getKey()).getScope(),
                                                                Stream.of(e.getKey()).getStreamName(),
                                                                e.getValue().getStreamCut()))));
        return ReaderGroupConfig.builder().readerGroupId(request.getReaderGroupId())
                .groupRefreshTimeMillis(request.getGroupRefreshTimeMillis())
                .automaticCheckpointIntervalMillis(request.getAutomaticCheckpointIntervalMillis())
                .maxOutstandingCheckpointRequest(request.getMaxOutstandingCheckpointRequest())
                .generation(request.getGeneration())
                .retentionType(ReaderGroupConfig.StreamDataRetention.values()[request.getRetentionTypeOrdinal()])
                .startingStreamCuts(startStreamCut)
                .endingStreamCuts(endStreamCut).build();
    }
}
