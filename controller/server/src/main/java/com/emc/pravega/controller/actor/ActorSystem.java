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
package com.emc.pravega.controller.actor;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;
import com.emc.pravega.stream.StreamManagerImpl;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
public class ActorSystem {

    private final String name;
    private final String hostName;
    private final List<ActorGroup> actorGroups;

    private final String scope;
    protected final Controller controller;
    protected final ClientFactory clientFactory;
    protected final StreamManager streamManager;

    private final Executor executor;

    public ActorSystem(String name, String hostName, String scope, Controller controller) {
        this.name = name;
        this.hostName = hostName;
        this.actorGroups = new ArrayList<>();

        this.scope = scope;
        this.controller = controller;
        this.streamManager = new StreamManagerImpl(scope, controller);
        this.clientFactory = new ClientFactoryImpl(scope, controller, new ConnectionFactoryImpl(false), streamManager);

        this.executor = Executors.newScheduledThreadPool(5);
    }

    public ActorGroupRef getActorSelection(String scope, String stream) {
        return new ActorGroupRef(this, scope, stream);
    }

    public ActorGroupRef actorOf(Props props) {
        ActorGroup actorGroup;
        try {

            // Create the actor group and start it.
            actorGroup = new ActorGroup(this, executor, props);
            actorGroup.startAsync();

        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            log.error("Error creating actor group.", e);
            return null;
        }
        return actorGroup.getRef();
    }

    public void notifyHostFailure(String host) {
        Preconditions.checkNotNull(host);
        if (host.equals(this.hostName)) {
            // shutdown all actor groups
        } else {
            // Notify all registered actor groups of host failure
            this.actorGroups.forEach(group -> group.notifyHostFailure(host));
        }
    }
}
