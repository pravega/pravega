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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.eventProcessor.impl.SerializedRequestHandler;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.AbortEvent;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;

/**
 * This actor processes commit txn events.
 * It does the following 2 operations in order.
 * 1. Send abort txn message to active segments of the stream.
 * 2. Change txn state from aborting to aborted.
 */
@Slf4j
public class AbortRequestHandler extends SerializedRequestHandler<AbortEvent> {
    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executor;

    private final BlockingQueue<AbortEvent> processedEvents;

    @VisibleForTesting
    public AbortRequestHandler(final StreamMetadataStore streamMetadataStore,
                               final StreamMetadataTasks streamMetadataTasks,
                               final ScheduledExecutorService executor,
                               final BlockingQueue<AbortEvent> queue) {
        super(executor);
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.executor = executor;
        this.processedEvents = queue;
    }

    public AbortRequestHandler(final StreamMetadataStore streamMetadataStore,
                               final StreamMetadataTasks streamMetadataTasks,
                               final ScheduledExecutorService executor) {
        super(executor);
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.executor = executor;
        this.processedEvents = null;
    }

    @Override
    public CompletableFuture<Void> processEvent(AbortEvent event) {
        String scope = event.getScope();
        String stream = event.getStream();
        int epoch = event.getEpoch();
        UUID txId = event.getTxid();
        long requestId = event.getRequestId();
        OperationContext context = streamMetadataStore.createContext(scope, stream);
        LoggerHelpers.debugLogWithTag(log, requestId, "Aborting transaction {} on stream {}/{}",
                event.getTxid(), event.getScope(), event.getStream());

        return Futures.toVoid(streamMetadataStore.getActiveSegmentIds(event.getScope(), event.getStream(), epoch, context, executor)
                .thenCompose(segments -> streamMetadataTasks.notifyTxnAbort(scope, stream, segments, txId, requestId))
                .thenCompose(x -> streamMetadataStore.abortTransaction(scope, stream, txId, context, executor))
                .whenComplete((result, error) -> {
                    if (error != null) {
                        LoggerHelpers.errorLogWithTag(log, requestId, "Failed aborting transaction {} on stream {}/{}",
                                event.getTxid(), event.getScope(), event.getStream());
                    } else {
                        LoggerHelpers.debugLogWithTag(log, requestId, "Successfully aborted transaction {} on stream {}/{}",
                                event.getTxid(), event.getScope(), event.getStream());
                        if (processedEvents != null) {
                            processedEvents.offer(event);
                        }
                    }
                }));
    }
}
