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


import io.pravega.controller.store.kvtable.TableMetadataStore;
import io.pravega.shared.controller.event.*;
import io.pravega.shared.controller.event.kvtable.CreateTableEvent;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class TableRequestHandler extends AbstractTableRequestProcessor<ControllerEvent> {

    private final CreateTableTask createTask;

    public TableRequestHandler(CreateTableTask createTask, TableMetadataStore store, ScheduledExecutorService executor) {
        super(store, executor);
        this.createTask = createTask;
    }

    @Override
    public CompletableFuture<Void> processCreateKVTable(CreateTableEvent createKVTEvent) {
        log.info("Processing create request {} for KeyValueTable {}/{}",
                createKVTEvent.getRequestId(), createKVTEvent.getScopeName(), createKVTEvent.getKvtName());
        return createTask.execute(createKVTEvent);
    }
}
