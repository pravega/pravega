/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.Props;
import com.emc.pravega.controller.eventProcessor.StreamEvent;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.Controller;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
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
        this.streamManager = StreamManager.withScope(scope, controller);
        this.clientFactory = new ClientFactoryImpl(scope, controller);

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

    public <T extends StreamEvent> EventProcessorGroup<T> createEventProcessorGroup(Props<T> props) {
        EventProcessorGroupImpl<T> actorGroup;

        // Create event processor group.
        actorGroup = new EventProcessorGroupImpl<>(this, props);

        // Initialize it.
        actorGroup.initialize();

        // If successful in initializing it, add it to the list and start it.
        synchronized (actorGroup) {
            actorGroups.add(actorGroup);
        }

        actorGroup.startAsync();

        return actorGroup;
    }

    @Override
    public List<EventProcessorGroup> getEventProcessorGroups() {
        return actorGroups;
    }
}
