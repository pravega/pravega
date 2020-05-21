/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers.kvtable;

import io.pravega.common.Exceptions;
import io.pravega.controller.server.eventProcessor.requesthandlers.AbstractRequestProcessor;
import io.pravega.controller.server.eventProcessor.requesthandlers.TaskExceptions;
import io.pravega.controller.server.eventProcessor.requesthandlers.stream.*;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.EpochTransitionOperationExceptions;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.shared.controller.event.*;
import io.pravega.shared.controller.event.kvtable.CreateKVTableEvent;
import io.pravega.shared.controller.event.stream.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class KVTableRequestHandler extends AbstractRequestProcessor<ControllerEvent> {

    private final CreateKVTableTask createKVTTask;

    public KVTableRequestHandler(CreateKVTableTask createKVTTask,
                                 KVTableMetadataStore streamMetadataStore,
                                 ScheduledExecutorService executor) {
        super(streamMetadataStore, executor);
        this.createKVTTask = createKVTTask;
    }

    @Override
    public boolean toPostpone(ControllerEvent event, long pickupTime, Throwable exception) {
        // We will let the event be postponed for 2 minutes before declaring failure.
        return Exceptions.unwrap(exception) instanceof TaskExceptions.StartException &&
                (System.currentTimeMillis() - pickupTime) < Duration.ofMinutes(2).toMillis();
    }

    @Override
    public CompletableFuture<Void> processCreateKVTable(CreateKVTableEvent createKVTEvent) {
        log.info("Processing create request {} for KeyValueTable {}/{}",
                createKVTEvent.getRequestId(), createKVTEvent.getScopeName(), createKVTEvent.getKvtName());
        return withCompletion(createKVTTask, createKVTEvent, createKVTEvent.getScopeName(), createKVTEvent.getKvtName(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info("Processing truncate request {} for stream {}/{} complete",
                            createKVTEvent.getRequestId(), createKVTEvent.getScopeName(), createKVTEvent.getKvtName());
                });
    }
}
