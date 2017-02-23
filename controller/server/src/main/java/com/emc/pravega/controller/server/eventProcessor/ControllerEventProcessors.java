/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.controller.eventProcessor.CheckpointStoreException;
import com.emc.pravega.controller.eventProcessor.EventProcessorConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import com.emc.pravega.controller.eventProcessor.ExceptionHandler;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class ControllerEventProcessors {

    private static EventProcessorSystem system;
    private static EventStreamWriter<CommitEvent> commitEventProcessors;
    private static EventStreamWriter<AbortEvent> abortEventProcessors;

    public static void initialize(final String host,
                                  final Controller controller,
                                  final CuratorFramework client,
                                  final StreamMetadataStore streamMetadataStore,
                                  final HostControllerStore hostControllerStore) throws Exception {

        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

        final String controllerScope = "system";
        system = new EventProcessorSystemImpl("Controller", host, controllerScope, controller);

        // Commit event processor configuration
        final String commitStream = "commitStream";
        final String commitStreamReaderGroup = "commitStreamReaders";
        final int commitReaderGroupSize = 5;
        final int commitPositionPersistenceFrequency = 10;

        // Abort event processor configuration
        final String abortStream = "abortStream";
        final String abortStreamReaderGroup = "abortStreamReaders";
        final int abortReaderGroupSize = 5;
        final int abortPositionPersistenceFrequency = 10;

        // Retry configuration
        final long delay = 100;
        final int multiplier = 10;
        final int attempts = 5;
        final long maxDelay = 10000;

        // region Create commit and abort streams

        ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 0L, 0, 5);
        StreamConfiguration commitStreamConfig =
                StreamConfiguration.builder()
                        .scope(controllerScope)
                        .streamName(commitStream)
                        .scalingPolicy(policy)
                        .build();

        StreamConfiguration abortStreamConfig =
                StreamConfiguration.builder()
                        .scope(controllerScope)
                        .streamName(abortStream)
                        .scalingPolicy(policy)
                        .build();

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
                        .storeType(CheckpointConfig.StoreType.Zookeeper)
                        .checkpointStoreClient(client)
                        .build();

        EventProcessorGroupConfig commitReadersConfig =
                EventProcessorGroupConfigImpl.builder()
                        .streamName(commitStream)
                        .readerGroupName(commitStreamReaderGroup)
                        .eventProcessorCount(commitReaderGroupSize)
                        .checkpointConfig(commitEventCheckpointConfig)
                        .build();

        EventProcessorConfig<CommitEvent> commitConfig =
                EventProcessorConfig.<CommitEvent>builder()
                        .config(commitReadersConfig)
                        .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                        .serializer(new JavaSerializer<>())
                        .supplier(() -> new CommitEventProcessor(streamMetadataStore, hostControllerStore, executor))
                        .build();

        Retry.withExpBackoff(delay, multiplier, attempts, maxDelay)
                .retryingOn(CheckpointStoreException.class)
                .throwingOn(Exception.class)
                .run(() -> {
                    commitEventProcessors = system.createEventProcessorGroup(commitConfig).getWriter();
                    return null;
                });

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
                        .storeType(CheckpointConfig.StoreType.Zookeeper)
                        .checkpointStoreClient(client)
                        .build();

        EventProcessorGroupConfig abortReadersConfig =
                EventProcessorGroupConfigImpl.builder()
                        .streamName(abortStream)
                        .readerGroupName(abortStreamReaderGroup)
                        .eventProcessorCount(abortReaderGroupSize)
                        .checkpointConfig(abortEventCheckpointConfig)
                        .build();

        EventProcessorConfig<AbortEvent> abortConfig =
                EventProcessorConfig.<AbortEvent>builder()
                        .config(abortReadersConfig)
                        .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                        .serializer(new JavaSerializer<>())
                        .supplier(() -> new AbortEventProcessor(streamMetadataStore, hostControllerStore, executor))
                        .build();

        Retry.withExpBackoff(delay, multiplier, attempts, maxDelay)
                .retryingOn(CheckpointStoreException.class)
                .throwingOn(Exception.class)
                .run(() -> {
                    abortEventProcessors = system.createEventProcessorGroup(abortConfig).getWriter();
                    return null;
                });

        // endregion
    }

    public static EventStreamWriter<CommitEvent> getCommitEventProcessorsRef() {
        return commitEventProcessors;
    }

    public static EventStreamWriter<AbortEvent> getAbortEventProcessorsRef() {
        return abortEventProcessors;
    }
}
