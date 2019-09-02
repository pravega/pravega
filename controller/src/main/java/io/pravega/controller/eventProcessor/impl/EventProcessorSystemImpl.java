/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.eventProcessor.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroup;
import io.pravega.controller.eventProcessor.EventProcessorSystem;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.shared.controller.event.ControllerEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventProcessorSystemImpl implements EventProcessorSystem {

    final EventStreamClientFactory clientFactory;
    final ReaderGroupManager readerGroupManager;

    private final String name;
    private final String process;

    private final String scope;

    public EventProcessorSystemImpl(String name, String process, String scope, EventStreamClientFactory clientFactory, ReaderGroupManager readerGroupManager) {
        this.name = name;
        this.process = process;

        this.scope = scope;
        this.clientFactory = clientFactory;
        this.readerGroupManager = readerGroupManager;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getScope() {
        return this.scope;
    }

    @Override
    public String getProcess() {
        return this.process;
    }

    @Override
    public <T extends ControllerEvent> EventProcessorGroup<T> createEventProcessorGroup(
            final EventProcessorConfig<T> eventProcessorConfig,
            final CheckpointStore checkpointStore) throws CheckpointStoreException {
        Preconditions.checkNotNull(eventProcessorConfig, "eventProcessorConfig");
        Preconditions.checkNotNull(checkpointStore, "checkpointStore");

        EventProcessorGroupImpl<T> actorGroup;

        // Create event processor group.
        actorGroup = new EventProcessorGroupImpl<>(this, eventProcessorConfig, checkpointStore);

        // Initialize it.
        actorGroup.initialize();

        actorGroup.startAsync();

        return actorGroup;
    }
}
