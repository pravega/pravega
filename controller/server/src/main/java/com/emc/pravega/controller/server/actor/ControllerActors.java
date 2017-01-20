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
package com.emc.pravega.controller.server.actor;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.zkImpl.ClusterZKImpl;
import com.emc.pravega.controller.actor.ActorGroupConfig;
import com.emc.pravega.controller.actor.ActorGroupRef;
import com.emc.pravega.controller.actor.ActorSystem;
import com.emc.pravega.controller.actor.Props;
import com.emc.pravega.controller.actor.impl.ActorGroupConfigImpl;
import com.emc.pravega.controller.actor.impl.ActorSystemImpl;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.stream.impl.Controller;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

// todo: use config values for constants defined in this file

@Slf4j
public class ControllerActors {

    private final ActorSystem system;
    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final Host host;
    private final CuratorFramework client;
    private final ClusterZKImpl clusterZK;
    private ActorGroupRef commitActors;

    // This executor is used to process callbacks from
    // ClusterZKImpl, ZK connection listener, and Actor's ServiceEventListener
    private final Executor executor;

    public ControllerActors(final Host host,
                            final String clusterName,
                            final CuratorFramework client,
                            final Controller controller,
                            final StreamMetadataStore streamMetadataStore,
                            final HostControllerStore hostControllerStore) {

        this.executor = Executors.newScheduledThreadPool(5);
        final String controllerScope = "system";
        system = new ActorSystemImpl("Controller", host, controllerScope, controller, executor);
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.host = host;
        this.client = client;
        this.clusterZK = new ClusterZKImpl(client, clusterName);
    }

    public void initialize() {

        //region cluster management
        clusterZK.registerHost(host);
        try {
            clusterZK.addListener(new ActorSystemClusterListener(system), executor);
        } catch (Exception e) {
            log.error("Error adding listener to cluster events", e);
            throw new RuntimeException(e);
        }

        ZKConnectionListener connectionListener = new ZKConnectionListener(host, system);
        client.getConnectionStateListenable().addListener(connectionListener, executor);
        //endregion

        // todo: create commitStream, if it does not exist
        final String commitStream = "commitStream";
        final String commitStreamReaderGroup = "commitStreamReaders";
        final int commitReaderGroupSize = 25;
        final int commitPositionPersistenceFrequency = 10;

        ActorGroupConfig commitReadersConfig =
                ActorGroupConfigImpl.builder()
                        .streamName(commitStream)
                        .readerGroupName(commitStreamReaderGroup)
                        .actorCount(commitReaderGroupSize)
                        .checkpointFrequency(commitPositionPersistenceFrequency)
                        .build();

        Props<CommitEvent> commitProps =
                new Props<>(commitReadersConfig,
                        null,
                        CommitEvent.getSerializer(),
                        CommitActor.class,
                        streamMetadataStore,
                        hostControllerStore);

        commitActors = system.actorOf(commitProps);
    }

    public ActorGroupRef getCommitActorGroupRef() {
        return commitActors;
    }
}
