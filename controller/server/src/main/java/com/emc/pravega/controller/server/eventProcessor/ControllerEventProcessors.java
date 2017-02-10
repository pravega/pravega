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
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.controller.eventProcessor.Decider;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import com.emc.pravega.controller.eventProcessor.Props;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.impl.Controller;
import lombok.extern.slf4j.Slf4j;

// todo: use config values for constants defined in this file

@Slf4j
public class ControllerEventProcessors {

    private static EventProcessorSystem system;
    private static EventStreamWriter<CommitEvent> commitEventProcessors;
    private static EventStreamWriter<AbortEvent> abortEventProcessors;

    public static void initialize(final String host,
                                  final Controller controller,
                                  final StreamMetadataStore streamMetadataStore,
                                  final HostControllerStore hostControllerStore) {

        final String controllerScope = "system";
        system = new EventProcessorSystemImpl("Controller", host, controllerScope, controller);

        // todo: create commitStream, if it does not exist
        final String commitStream = "commitStream";
        final String commitStreamReaderGroup = "commitStreamReaders";
        final int commitReaderGroupSize = 5;
        final int commitPositionPersistenceFrequency = 10;

        CheckpointConfig commitEventCheckpointConfig =
                CheckpointConfig.builder()
                        .type(CheckpointConfig.Type.Periodic)
                        .checkpointPeriod(
                                CheckpointConfig.CheckpointPeriod.builder()
                                        .numEvents(commitPositionPersistenceFrequency)
                                        .numSeconds(commitPositionPersistenceFrequency)
                                        .build())
                        .build();

        EventProcessorGroupConfig commitReadersConfig =
                EventProcessorGroupConfigImpl.builder()
                        .streamName(commitStream)
                        .readerGroupName(commitStreamReaderGroup)
                        .actorCount(commitReaderGroupSize)
                        .checkpointConfig(commitEventCheckpointConfig)
                        .build();

        Props<CommitEvent> commitProps =
                Props.<CommitEvent>builder()
                        .config(commitReadersConfig)
                        .decider(Decider.DEFAULT_DECIDER)
                        .serializer(CommitEvent.getSerializer())
                        .clazz(CommitEventProcessor.class)
                        .args(streamMetadataStore, hostControllerStore)
                        .build();

        commitEventProcessors = system.createEventProcessorGroup(commitProps);

        // todo: create commitStream, if it does not exist
        final String abortStream = "abortStream";
        final String abortStreamReaderGroup = "abortStreamReaders";
        final int abortReaderGroupSize = 5;
        final int abortPositionPersistenceFrequency = 10;

        CheckpointConfig abortEventCheckpointConfig =
                CheckpointConfig.builder()
                        .type(CheckpointConfig.Type.Periodic)
                        .checkpointPeriod(
                                CheckpointConfig.CheckpointPeriod.builder()
                                        .numEvents(abortPositionPersistenceFrequency)
                                        .numSeconds(abortPositionPersistenceFrequency)
                                        .build())
                        .build();

        EventProcessorGroupConfig abortReadersConfig =
                EventProcessorGroupConfigImpl.builder()
                        .streamName(abortStream)
                        .readerGroupName(abortStreamReaderGroup)
                        .actorCount(abortReaderGroupSize)
                        .checkpointConfig(abortEventCheckpointConfig)
                        .build();

        Props<AbortEvent> abortProps =
                Props.<AbortEvent>builder()
                        .config(abortReadersConfig)
                        .decider(Decider.DEFAULT_DECIDER)
                        .serializer(AbortEvent.getSerializer())
                        .clazz(AbortEventProcessor.class)
                        .args(streamMetadataStore, hostControllerStore)
                        .build();

        abortEventProcessors = system.createEventProcessorGroup(abortProps);
    }

    public static EventStreamWriter<CommitEvent> getCommitEventProcessorsRef() {
        return commitEventProcessors;
    }

    public static EventStreamWriter<AbortEvent> getAbortEventProcessorsRef() {
        return abortEventProcessors;
    }
}
