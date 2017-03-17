/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.store.checkpoint.CheckpointStore;
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
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class ControllerEventProcessors extends AbstractIdleService {

    public static final Serializer<CommitEvent> COMMIT_EVENT_SERIALIZER = new JavaSerializer<>();
    public static final Serializer<AbortEvent> ABORT_EVENT_SERIALIZER = new JavaSerializer<>();

    // Retry configuration
    private static final long DELAY = 100;
    private static final int MULTIPLIER = 10;
    private static final long MAX_DELAY = 10000;

    private final String objectId;
    private final ControllerEventProcessorConfig config;
    private final CheckpointStore checkpointStore;
    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final EventProcessorSystem system;
    private final SegmentHelper segmentHelper;
    private final ScheduledExecutorService executor;

    private EventProcessorGroup<CommitEvent> commitEventEventProcessors;
    private EventProcessorGroup<AbortEvent> abortEventEventProcessors;

    public ControllerEventProcessors(final String host,
                                     final ControllerEventProcessorConfig config,
                                     final Controller controller,
                                     final CheckpointStore checkpointStore,
                                     final StreamMetadataStore streamMetadataStore,
                                     final HostControllerStore hostControllerStore,
                                     final SegmentHelper segmentHelper,
                                     final ScheduledExecutorService executor) {
        this.objectId  = "ControllerEventProcessors";
        this.config = config;
        this.checkpointStore = checkpointStore;
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
        this.system = new EventProcessorSystemImpl("Controller", host, config.getScopeName(), controller);
    }

    @Override
    protected void startUp() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, this.objectId, "startUp");
        try {
            log.info("Starting controller event processors");
            initialize();
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    @Override
    protected void shutDown() {
        long traceId = LoggerHelpers.traceEnter(log, this.objectId, "shutDown");
        try {
            log.info("Stopping controller event processors.");
            stopEventProcessors();
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
        }
    }

    private static CompletableFuture<Void> createStreams(final Controller controller,
                                                         final ControllerEventProcessorConfig config,
                                                         final ScheduledExecutorService executor) {
        StreamConfiguration commitStreamConfig =
                StreamConfiguration.builder()
                        .scope(config.getScopeName())
                        .streamName(config.getCommitStreamName())
                        .scalingPolicy(config.getCommitStreamScalingPolicy())
                        .build();

        StreamConfiguration abortStreamConfig =
                StreamConfiguration.builder()
                        .scope(config.getScopeName())
                        .streamName(config.getAbortStreamName())
                        .scalingPolicy(config.getAbortStreamScalingPolicy())
                        .build();

        return createScope(controller, config.getScopeName(), executor)
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
                                throw new RuntimeException("Error creating commitStream");
                            } else {
                                return null;
                            }
                        }), executor);
    }


    public static CompletableFuture<Void> bootstrap(final Controller localController,
                                                    final ControllerEventProcessorConfig config,
                                                    final StreamTransactionMetadataTasks streamTransactionMetadataTasks,
                                                    final ScheduledExecutorService executor) {
        return ControllerEventProcessors.createStreams(localController, config, executor)
                .thenAcceptAsync(x -> streamTransactionMetadataTasks.initializeStreamWriters(localController, config),
                        executor);
    }

    private void initialize() throws Exception {

        // region Create commit event processor

        EventProcessorGroupConfig commitReadersConfig =
                EventProcessorGroupConfigImpl.builder()
                        .streamName(config.getCommitStreamName())
                        .readerGroupName(config.getCommitReaderGroupName())
                        .eventProcessorCount(config.getCommitReaderGroupSize())
                        .checkpointConfig(config.getCommitCheckpointConfig())
                        .build();

        EventProcessorConfig<CommitEvent> commitConfig =
                EventProcessorConfig.<CommitEvent>builder()
                        .config(commitReadersConfig)
                        .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                        .serializer(COMMIT_EVENT_SERIALIZER)
                        .supplier(() -> new CommitEventProcessor(streamMetadataStore, hostControllerStore, executor, segmentHelper))
                        .build();

        log.info("Creating commit event processors");
        Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating commit event processor group", e))
                .run(() -> {
                    commitEventEventProcessors = system.createEventProcessorGroup(commitConfig, checkpointStore);
                    return null;
                });

        // endregion

        // region Create abort event processor

        EventProcessorGroupConfig abortReadersConfig =
                EventProcessorGroupConfigImpl.builder()
                        .streamName(config.getAbortStreamName())
                        .readerGroupName(config.getAbortReaderGrouopName())
                        .eventProcessorCount(config.getAbortReaderGroupSize())
                        .checkpointConfig(config.getAbortCheckpointConfig())
                        .build();

        EventProcessorConfig<AbortEvent> abortConfig =
                EventProcessorConfig.<AbortEvent>builder()
                        .config(abortReadersConfig)
                        .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                        .serializer(ABORT_EVENT_SERIALIZER)
                        .supplier(() -> new AbortEventProcessor(streamMetadataStore, hostControllerStore, executor, segmentHelper))
                        .build();

        log.info("Creating abort event processors");
        Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating commit event processor group", e))
                .run(() -> {
                    abortEventEventProcessors = system.createEventProcessorGroup(abortConfig, checkpointStore);
                    return null;
                });

        // endregion

        log.info("Awaiting start of commit event processors");
        commitEventEventProcessors.awaitRunning();
        log.info("Awaiting start of abort event processors");
        abortEventEventProcessors.awaitRunning();
    }

    private void stopEventProcessors() {
        if (commitEventEventProcessors != null) {
            log.info("Stopping commit event processors");
            commitEventEventProcessors.stopAsync();
        }
        if (abortEventEventProcessors != null) {
            log.info("Stopping abort event processors");
            abortEventEventProcessors.stopAsync();
        }
        if (commitEventEventProcessors != null) {
            log.info("Awaiting termination of commit event processors");
            commitEventEventProcessors.awaitTerminated();
        }
        if (abortEventEventProcessors != null) {
            log.info("Awaiting termination of abort event processors");
            abortEventEventProcessors.awaitTerminated();
        }
    }
}
