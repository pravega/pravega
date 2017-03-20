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
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import com.emc.pravega.controller.eventProcessor.ExceptionHandler;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.google.common.util.concurrent.AbstractService;
import lombok.Lombok;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class ControllerEventProcessors extends AbstractService {

    public static final String CONTROLLER_SCOPE = "system";
    public static final String COMMIT_STREAM = "commitStream";
    public static final String ABORT_STREAM = "abortStream";
    public static final Serializer<CommitEvent> COMMIT_EVENT_SERIALIZER = new JavaSerializer<>();
    public static final Serializer<AbortEvent> ABORT_EVENT_SERIALIZER = new JavaSerializer<>();

    // Retry configuration
    private static final long DELAY = 100;
    private static final int MULTIPLIER = 10;
    private static final long MAX_DELAY = 10000;

    private final CuratorFramework client;
    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final EventProcessorSystem system;
    private final SegmentHelper segmentHelper;
    private final ScheduledExecutorService executor;

    private EventProcessorGroup<CommitEvent> commitEventEventProcessors;
    private EventProcessorGroup<AbortEvent> abortEventEventProcessors;

    public ControllerEventProcessors(final String host,
                                     final Controller controller,
                                     final CuratorFramework client,
                                     final StreamMetadataStore streamMetadataStore,
                                     final HostControllerStore hostControllerStore,
                                     final SegmentHelper segmentHelper,
                                     final ScheduledExecutorService executor) {
        this.client = client;
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
        this.system = new EventProcessorSystemImpl("Controller", host, CONTROLLER_SCOPE, controller);
    }

    @Override
    protected void doStart() {
        try {
            log.info("Starting controller event processors.");
            initialize();
            notifyStarted();
        } catch (Exception e) {
            log.error("Error starting controller event processors.", e);
            // Throwing this error will mark the service as FAILED.
            throw Lombok.sneakyThrow(e);
        }
    }

    @Override
    protected void doStop() {
        try {
            log.info("Stopping controller event processors.");
            stopEventProcessors();
        } catch (CheckpointStoreException e) {
            log.error("Error stopping controller event processors.", e);
        }
    }

    private static CompletableFuture<Void> createStreams(final Controller controller,
                                                         final ScheduledExecutorService executor) {
        StreamConfiguration commitStreamConfig =
                StreamConfiguration.builder()
                        .scope(CONTROLLER_SCOPE)
                        .streamName(COMMIT_STREAM)
                        .scalingPolicy(ScalingPolicy.fixed(2))
                        .build();

        StreamConfiguration abortStreamConfig =
                StreamConfiguration.builder()
                        .scope(CONTROLLER_SCOPE)
                        .streamName(ABORT_STREAM)
                        .scalingPolicy(ScalingPolicy.fixed(2))
                        .build();

        return createScope(controller, CONTROLLER_SCOPE, executor)
                .thenCompose(ignore ->
                        CompletableFuture.allOf(createStream(controller, commitStreamConfig, executor),
                                createStream(controller, abortStreamConfig, executor)));
    }

    private static CompletableFuture<Void> createScope(final Controller controller,
                                                       final String scopeName,
                                                       final ScheduledExecutorService executor) {
        return Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating event processor scope " + scopeName, e))
                .runAsync(() -> controller.createScope(scopeName)
                        .thenApply(result -> {
                                    if (CreateScopeStatus.Status.FAILURE == result.getStatus()) {
                                        throw new RuntimeException("Error creating scope " + scopeName);
                                    } else {
                                        return null;
                                    }
                                }
                        ), executor);
    }

    private static CompletableFuture<Void> createStream(final Controller controller,
                                                        final StreamConfiguration streamConfig,
                                                        final ScheduledExecutorService executor) {
        return Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating event processor stream " + streamConfig.getStreamName(), e))
                .runAsync(() -> controller.createStream(streamConfig)
                        .thenApply(result -> {
                            if (CreateStreamStatus.Status.FAILURE == result.getStatus()) {
                                throw new RuntimeException("Error creating stream " +
                                        streamConfig.getScope() + "/" + streamConfig.getStreamName());
                            } else {
                                return null;
                            }
                        }), executor);
    }


    public static CompletableFuture<Void> bootstrap(final Controller localController,
                                                    final StreamTransactionMetadataTasks streamTransactionMetadataTasks,
                                                    final ScheduledExecutorService executor) {
        return ControllerEventProcessors.createStreams(localController, executor)
                .thenAcceptAsync(x -> streamTransactionMetadataTasks.initializeStreamWriters(localController),
                        executor);
    }

    private void initialize() throws Exception {

        // Commit event processor configuration
        final String commitStreamReaderGroup = "commitStreamReaders";
        final int commitReaderGroupSize = 1;
        final int commitPositionPersistenceFrequency = 10;

        // Abort event processor configuration
        final String abortStreamReaderGroup = "abortStreamReaders";
        final int abortReaderGroupSize = 1;
        final int abortPositionPersistenceFrequency = 10;

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
                        .streamName(COMMIT_STREAM)
                        .readerGroupName(commitStreamReaderGroup)
                        .eventProcessorCount(commitReaderGroupSize)
                        .checkpointConfig(commitEventCheckpointConfig)
                        .build();

        EventProcessorConfig<CommitEvent> commitConfig =
                EventProcessorConfig.<CommitEvent>builder()
                        .config(commitReadersConfig)
                        .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                        .serializer(COMMIT_EVENT_SERIALIZER)
                        .supplier(() -> new CommitEventProcessor(streamMetadataStore, hostControllerStore, executor, segmentHelper))
                        .build();

        Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating commit event processor group", e))
                .run(() -> {
                    commitEventEventProcessors = system.createEventProcessorGroup(commitConfig);
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
                        .streamName(ABORT_STREAM)
                        .readerGroupName(abortStreamReaderGroup)
                        .eventProcessorCount(abortReaderGroupSize)
                        .checkpointConfig(abortEventCheckpointConfig)
                        .build();

        EventProcessorConfig<AbortEvent> abortConfig =
                EventProcessorConfig.<AbortEvent>builder()
                        .config(abortReadersConfig)
                        .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                        .serializer(ABORT_EVENT_SERIALIZER)
                        .supplier(() -> new AbortEventProcessor(streamMetadataStore, hostControllerStore, executor, segmentHelper))
                        .build();

        Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating commit event processor group", e))
                .run(() -> {
                    abortEventEventProcessors = system.createEventProcessorGroup(abortConfig);
                    return null;
                });

        // endregion
    }

    private void stopEventProcessors() throws CheckpointStoreException {
        if (commitEventEventProcessors != null) {
            commitEventEventProcessors.stopAll();
        }
        if (abortEventEventProcessors != null) {
            abortEventEventProcessors.stopAll();
        }
    }
}
