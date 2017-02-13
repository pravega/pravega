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
import com.emc.pravega.controller.eventProcessor.CheckpointStore;
import com.emc.pravega.controller.eventProcessor.Decider;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import com.emc.pravega.controller.eventProcessor.Props;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;

// todo: use config values for constants defined in this file

@Slf4j
public class ControllerEventProcessors {

    private static EventProcessorSystem system;
    private static EventStreamWriter<CommitEvent> commitEventProcessors;
    private static EventStreamWriter<AbortEvent> abortEventProcessors;

    public static void initialize(final String host,
                                  final Controller controller,
                                  final CuratorFramework client,
                                  final StreamMetadataStore streamMetadataStore,
                                  final HostControllerStore hostControllerStore) {

        final String controllerScope = "system";
        system = new EventProcessorSystemImpl("Controller", host, controllerScope, controller);

        final String commitStream = "commitStream";
        final String commitStreamReaderGroup = "commitStreamReaders";
        final int commitReaderGroupSize = 5;
        final int commitPositionPersistenceFrequency = 10;

        final String abortStream = "abortStream";
        final String abortStreamReaderGroup = "abortStreamReaders";
        final int abortReaderGroupSize = 5;
        final int abortPositionPersistenceFrequency = 10;

        // region Create commit and abort streams

        ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 0L, 0, 5);
        StreamConfiguration commitStreamConfig = new StreamConfigurationImpl(controllerScope, commitStream, policy);
        StreamConfiguration abortStreamConfig = new StreamConfigurationImpl(controllerScope, abortStream, policy);

        CompletableFuture<CreateStreamStatus> createCommitStreamStatus = controller.createStream(commitStreamConfig);
        CompletableFuture<CreateStreamStatus> createAbortStreamStatus = controller.createStream(abortStreamConfig);

        CreateStreamStatus commitStreamStatus = createCommitStreamStatus.join();
        CreateStreamStatus abortStreamStatus = createAbortStreamStatus.join();

        if (CreateStreamStatus.FAILURE == commitStreamStatus) {
            throw new RuntimeException("Error creating commitStream");
        }

        if (CreateStreamStatus.FAILURE == abortStreamStatus) {
            throw new RuntimeException("Error creating abortStream");
        }

        // endregion

        // region Create commit event processor

        CheckpointConfig commitEventCheckpointConfig =
                CheckpointConfig.builder()
                        .type(CheckpointConfig.Type.Periodic)
                        .checkpointPeriod(
                                CheckpointConfig.CheckpointPeriod.builder()
                                        .numEvents(commitPositionPersistenceFrequency)
                                        .numSeconds(commitPositionPersistenceFrequency)
                                        .build())
                        .storeType(CheckpointStore.StoreType.Zookeeper)
                        .checkpointStoreClient(client)
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
                        .serializer(new JavaSerializer<>())
                        .clazz(CommitEventProcessor.class)
                        .args(streamMetadataStore, hostControllerStore)
                        .build();

        commitEventProcessors = system.createEventProcessorGroup(commitProps);

        // endregion

        // region Create abort event processor

        CheckpointConfig abortEventCheckpointConfig =
                CheckpointConfig.builder()
                        .type(CheckpointConfig.Type.Periodic)
                        .checkpointPeriod(
                                CheckpointConfig.CheckpointPeriod.builder()
                                        .numEvents(abortPositionPersistenceFrequency)
                                        .numSeconds(abortPositionPersistenceFrequency)
                                        .build())
                        .storeType(CheckpointStore.StoreType.Zookeeper)
                        .checkpointStoreClient(client)
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
                        .serializer(new JavaSerializer<>())
                        .clazz(AbortEventProcessor.class)
                        .args(streamMetadataStore, hostControllerStore)
                        .build();

        abortEventProcessors = system.createEventProcessorGroup(abortProps);

        // endregion
    }

    public static EventStreamWriter<CommitEvent> getCommitEventProcessorsRef() {
        return commitEventProcessors;
    }

    public static EventStreamWriter<AbortEvent> getAbortEventProcessorsRef() {
        return abortEventProcessors;
    }
}
