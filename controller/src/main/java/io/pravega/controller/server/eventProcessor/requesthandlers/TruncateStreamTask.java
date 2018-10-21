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
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.MetricsNames.TRUNCATED_SIZE;
import static io.pravega.shared.MetricsNames.nameFromStream;

/**
 * Request handler for performing truncation operations received from requeststream.
 */
@Slf4j
public class TruncateStreamTask implements StreamTask<TruncateStreamEvent> {
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

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

        return streamMetadataStore.getVersionedState(scope, stream, context, executor)
                .thenCompose(versionedState -> streamMetadataStore.getTruncationRecord(scope, stream, context, executor)
                        .thenCompose(versionedMetadata -> {
                            if (!versionedMetadata.getObject().isUpdating()) {
                                if (versionedState.getObject().equals(State.TRUNCATING)) {
                                    return Futures.toVoid(streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE,
                                            versionedState, context, executor));
                                } else {
                                    throw new TaskExceptions.StartException("Truncate Stream not started yet.");
                                }
                            } else {
                                return processTruncate(scope, stream, versionedMetadata, versionedState, context);
                            }
                        }));
    }

    private CompletableFuture<Void> processTruncate(String scope, String stream, VersionedMetadata<StreamTruncationRecord> versionedTruncationRecord,
                                                    VersionedMetadata<State> versionedState, OperationContext context) {
        String delegationToken = this.streamMetadataTasks.retrieveDelegationToken();
        StreamTruncationRecord truncationRecord = versionedTruncationRecord.getObject();
        log.info("Truncating stream {}/{} at stream cut: {}", scope, stream, truncationRecord.getStreamCut());
        return Futures.toVoid(streamMetadataStore.updateVersionedState(scope, stream, State.TRUNCATING, versionedState, context, executor)
                .thenCompose(update -> notifyTruncateSegments(scope, stream, truncationRecord.getStreamCut(), delegationToken)
                        .thenCompose(x -> notifyDeleteSegments(scope, stream, truncationRecord.getToDelete(), delegationToken))
                        .thenCompose(x -> streamMetadataStore.getSizeTillStreamCut(scope, stream, truncationRecord.getStreamCut(),
                                context, executor))
                        .thenAccept(truncatedSize -> DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(TRUNCATED_SIZE, scope, stream), truncatedSize))
                        .thenCompose(deleted -> streamMetadataStore.completeTruncation(scope, stream, versionedTruncationRecord, context, executor))
                        .thenCompose(x -> streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE, update, context, executor))));
    }

    private CompletableFuture<Void> notifyDeleteSegments(String scope, String stream, Set<Long> segmentsToDelete, String delegationToken) {
        log.debug("{}/{} deleting segments {}", scope, stream, segmentsToDelete);
        return Futures.allOf(segmentsToDelete.stream()
                .parallel()
                .map(segment -> streamMetadataTasks.notifyDeleteSegment(scope, stream, segment, delegationToken))
                .collect(Collectors.toList()));
    }

    private CompletableFuture<Void> notifyTruncateSegments(String scope, String stream, Map<Long, Long> streamCut, String delegationToken) {
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
