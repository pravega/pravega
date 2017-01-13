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
import com.emc.pravega.stream.impl.Controller;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ActorSystem {

    protected final Controller controller;
    protected final ClientFactory clientFactory;
    protected final StreamManager streamManager;
    private final Executor executor;

    public ActorSystem(Controller controller, ClientFactory clientFactory, StreamManager streamManager) {
        this.controller = controller;
        this.clientFactory = clientFactory;
        this.streamManager = streamManager;
        this.executor = Executors.newFixedThreadPool(5);
    }

    public ActorGroupRef getActorSelection(String scope, String stream) {
        return new ActorGroupRef(this, scope, stream);
    }

    public ActorGroupRef actorOf(Props props) {
        ActorGroup actorGroup;
        try {
            actorGroup = new ActorGroup(this, executor, props);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            return null;
        }
        return actorGroup.getRef();
    }
}
