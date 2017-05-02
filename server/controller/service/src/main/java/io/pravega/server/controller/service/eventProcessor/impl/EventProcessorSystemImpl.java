/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.eventProcessor.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.server.controller.service.store.checkpoint.CheckpointStore;
import io.pravega.server.controller.service.store.checkpoint.CheckpointStoreException;
import io.pravega.server.controller.service.eventProcessor.EventProcessorGroup;
import io.pravega.server.controller.service.eventProcessor.EventProcessorSystem;
import io.pravega.server.controller.service.eventProcessor.EventProcessorConfig;
import io.pravega.shared.controller.event.ControllerEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventProcessorSystemImpl implements EventProcessorSystem {

    final ClientFactory clientFactory;
    final ReaderGroupManager readerGroupManager;

    private final String name;
    private final String process;

    private final String scope;

    public EventProcessorSystemImpl(String name, String process, String scope, ClientFactory clientFactory, ReaderGroupManager readerGroupManager) {
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
