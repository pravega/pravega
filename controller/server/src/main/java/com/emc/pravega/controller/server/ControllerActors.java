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
package com.emc.pravega.controller.server;

import com.emc.pravega.controller.actor.ActorGroupConfig;
import com.emc.pravega.controller.actor.ActorGroupRef;
import com.emc.pravega.controller.actor.ActorSystem;
import com.emc.pravega.controller.actor.Props;
import com.emc.pravega.controller.actor.impl.ActorGroupConfigImpl;
import com.emc.pravega.controller.actor.impl.ActorSystemImpl;
import com.emc.pravega.controller.actor.impl.CommitActor;
import com.emc.pravega.controller.actor.impl.MetricsActor;
import com.emc.pravega.stream.impl.Controller;

public class ControllerActors {

    // todo: setup proper hostName
    private final String hostName = "tmp:hostName";
    private final String controllerScope = "system";
    private final ActorSystem system;
    private ActorGroupRef metricsActors;
    private ActorGroupRef commitActors;

    ControllerActors(Controller controller) {
        system = new ActorSystemImpl("Controller", hostName, controllerScope, controller);
    }

    void initialize() {

        // todo: create metricsStream, if it does not exist
        final String metricsStream = "metricsStream";
        final String metricsStreamReaderGroup = "metricsStreamReaders";
        final int metricsReaderGroupSize = 10;
        final int metricsPositionPersistenceFrequency = 100;

        ActorGroupConfig metricsReadersConfig = new ActorGroupConfigImpl(metricsStream, metricsStreamReaderGroup, metricsReaderGroupSize, metricsPositionPersistenceFrequency);
        Props metricsProps = new Props(metricsReadersConfig, null, MetricsActor.class);
        metricsActors = system.actorOf(metricsProps);

        // todo: create commitStream, if it does not exist
        final String commitStream = "commitStream";
        final String commitStreamReaderGroup = "commitStreamReaders";
        final int commitReaderGroupSize = 25;
        final int commitPositionPersistenceFrequency = 10;

        ActorGroupConfig commitReadersConfig = new ActorGroupConfigImpl(commitStream, commitStreamReaderGroup, commitReaderGroupSize, commitPositionPersistenceFrequency);
        Props commitProps = new Props(commitReadersConfig, null, CommitActor.class);
        commitActors = system.actorOf(commitProps);
    }

    public ActorGroupRef getMetricsActorGroupRef() {
        return metricsActors;
    }

    public ActorGroupRef getCommitActorGroupRef() {
        return commitActors;
    }
}
