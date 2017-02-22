/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;
import com.emc.pravega.controller.eventProcessor.CheckpointStoreException;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.EventProcessorConfig;
import com.emc.pravega.controller.eventProcessor.ControllerEvent;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.StreamManagerImpl;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class EventProcessorSystemImpl implements EventProcessorSystem {

    final Controller controller;
    final ClientFactory clientFactory;
    final StreamManager streamManager;

    private final String name;
    private final String process;
    @GuardedBy("actorGroups")
    private final List<EventProcessorGroup> actorGroups;

    private final String scope;

    public EventProcessorSystemImpl(String name, String process, String scope, Controller controller) {
        this.name = name;
        this.process = process;
        this.actorGroups = new ArrayList<>();

        this.scope = scope;
        this.controller = controller;
        this.clientFactory = new ClientFactoryImpl(scope, controller);
        this.streamManager = new StreamManagerImpl(scope, controller, clientFactory);
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

    public <T extends ControllerEvent> EventProcessorGroup<T> createEventProcessorGroup(EventProcessorConfig<T> eventProcessorConfig) throws CheckpointStoreException {
        EventProcessorGroupImpl<T> actorGroup;

        // Create event processor group.
        actorGroup = new EventProcessorGroupImpl<>(this, eventProcessorConfig);

        // Initialize it.
        actorGroup.initialize();

        // If successful in initializing it, add it to the list and start it.
        synchronized (actorGroups) {
            actorGroups.add(actorGroup);
        }

        actorGroup.startAsync();

        return actorGroup;
    }

    @Override
    public List<EventProcessorGroup> getEventProcessorGroups() {
        return Collections.unmodifiableList(actorGroups);
    }
}
