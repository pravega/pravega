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
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.Controller;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class EventProcessorSystemImpl implements EventProcessorSystem {

    protected final Controller controller;
    protected final ClientFactory clientFactory;
    protected final StreamManager streamManager;

    private final String name;
    private final Host host;
    @GuardedBy("actorGroups")
    private final List<EventProcessorGroupImpl> actorGroups;

    private final String scope;

    public EventProcessorSystemImpl(String name, Host host, String scope, Controller controller) {
        this.name = name;
        this.host = host;
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

    public <T extends StreamEvent> EventStreamWriter<T> actorOf(Props<T> props) {
        synchronized (actorGroups) {
            EventProcessorGroupImpl<T> actorGroup;

            // Create the actor group, add it to the list of actor groups, and start it.
            actorGroup = new EventProcessorGroupImpl<>(this, props);

            actorGroups.add(actorGroup);

            actorGroup.startAsync();

            return actorGroup.getSelf();
        }
    }

    @Override
    public void notifyHostFailure(Host host) {
        Preconditions.checkNotNull(host);
        if (host.equals(this.host)) {
            this.actorGroups.forEach(EventProcessorGroupImpl::stopAsync);
        } else {
            // Notify all registered actor groups of host failure
            this.actorGroups.forEach(group -> group.notifyHostFailure(host));
        }
    }
}
