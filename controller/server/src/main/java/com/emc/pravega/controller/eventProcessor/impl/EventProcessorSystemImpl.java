/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.EventProcessorConfig;
import com.emc.pravega.controller.eventProcessor.ControllerEvent;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.ReaderGroupManagerImpl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventProcessorSystemImpl implements EventProcessorSystem {

    final Controller controller;
    final ClientFactory clientFactory;
    final ReaderGroupManager readerGroupManager;

    private final String name;
    private final String process;

    private final String scope;

    public EventProcessorSystemImpl(String name, String process, String scope, Controller controller) {
        this.name = name;
        this.process = process;

        this.scope = scope;
        this.controller = controller;
        this.clientFactory = new ClientFactoryImpl(scope, controller);
        this.readerGroupManager = new ReaderGroupManagerImpl(scope, controller);
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
            final EventProcessorConfig<T> eventProcessorConfig) throws CheckpointStoreException {
        EventProcessorGroupImpl<T> actorGroup;

        // Create event processor group.
        actorGroup = new EventProcessorGroupImpl<>(this, eventProcessorConfig);

        // Initialize it.
        actorGroup.initialize();

        actorGroup.startAsync();

        return actorGroup;
    }
}
