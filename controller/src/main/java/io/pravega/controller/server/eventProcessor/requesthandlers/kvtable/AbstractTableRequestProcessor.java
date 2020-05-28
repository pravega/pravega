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

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.eventProcessor.impl.SerializedRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.RequestUnsupportedException;
import io.pravega.controller.store.kvtable.TableMetadataStore;

import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.kvtable.CreateTableEvent;
import io.pravega.shared.controller.event.kvtable.TableRequestProcessor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract common class for all request processing done over SerializedRequestHandler.
 * This implements TableRequestProcessor interface and implements failing request processing for all ControllerEvent types with
 * RequestUnsupported.
 * Its derived classes should implement specific processing that they wish to handle.
 *
 * This class provides a common completion method which implements mechanisms that allow multiple event processors
 * working on same stream to get fairness in their scheduling and avoid starvation.
 * To do this, before starting any processing, it fetches waiting request processor record from the store and if there
 * is a record in the store and it doesnt match current processing request, then it simply postpones the current processing.
 * Otherwise it attempts to process the event. If the event fails in its processing because of contention with another
 * process on a different processor, then it sets itself as the waiting request processor in the stream metadata. This will
 * ensure that when other event processors complete their processing, they will not pick newer work until this processor
 * processes at least one event.
 * At the end of processing, each processor attempts to clean up waiting request processor record from the store if it
 * was set against its name.
 */
@Slf4j
public abstract class AbstractTableRequestProcessor<T extends ControllerEvent> extends SerializedRequestHandler<T> implements TableRequestProcessor {

    @Getter
    protected final TableMetadataStore metadataStore;

    public AbstractTableRequestProcessor(TableMetadataStore store, ScheduledExecutorService executor) {
        super(executor);
        Preconditions.checkNotNull(store);
        this.metadataStore = store;
    }

    public String getProcessorName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public CompletableFuture<Void> processEvent(ControllerEvent controllerEvent) {
        return controllerEvent.process(this);
    }

    public CompletableFuture<Void> processCreateKVTable(CreateTableEvent createTableEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }
}
