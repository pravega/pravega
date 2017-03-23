/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.eventProcessor.ControllerEvent;
import com.emc.pravega.controller.store.checkpoint.CheckpointStore;
import com.emc.pravega.controller.eventProcessor.EventProcessorConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.ExceptionHandler;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
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
    private final ConnectionFactory connectionFactory;
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
                                     final ConnectionFactory connectionFactory,
                                     final ScheduledExecutorService executor) {
        this.objectId  = "ControllerEventProcessors";
        this.config = config;
        this.checkpointStore = checkpointStore;
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.connectionFactory = connectionFactory;
        this.system = new EventProcessorSystemImpl("Controller", host, config.getScopeName(), controller, connectionFactory);
        this.executor = executor;
    }

    public void notifyProcessFailure(String process) {
        try {
            if (commitEventEventProcessors != null) {
                commitEventEventProcessors.notifyProcessFailure(process);
            }
            if (abortEventEventProcessors != null) {
                abortEventEventProcessors.notifyProcessFailure(process);
            }
        } catch (CheckpointStoreException e) {
            log.error(String.format("Failed handling failure for host %s", process), e);
        }
    }

    @Override
    protected void startUp() throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startUp");
        try {
            log.info("Starting controller event processors");
            initialize();
            log.info("Controller event processors startUp complete");
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    @Override
    protected void shutDown() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "shutDown");
        try {
            log.info("Stopping controller event processors");
            stopEventProcessors();
            log.info("Controller event processors shutDown complete");
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
                .thenComposeAsync(ignore ->
                        CompletableFuture.allOf(createStream(controller, commitStreamConfig, executor),
                                createStream(controller, abortStreamConfig, executor)), executor);
    }

    private static CompletableFuture<Void> createScope(final Controller controller,
                                                       final String scopeName,
                                                       final ScheduledExecutorService executor) {
        return FutureHelpers.toVoid(Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating event processor scope " + scopeName, e))
                .runAsync(() -> controller.createScope(scopeName), executor));
    }

    private static CompletableFuture<Void> createStream(final Controller controller,
                                                        final StreamConfiguration streamConfig,
                                                        final ScheduledExecutorService executor) {
        return FutureHelpers.toVoid(Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating event processor stream " + streamConfig.getStreamName(), e))
                .runAsync(() -> controller.createStream(streamConfig), executor));
    }


    public static CompletableFuture<Void> bootstrap(final Controller localController,
                                                    final ControllerEventProcessorConfig config,
                                                    final StreamTransactionMetadataTasks streamTransactionMetadataTasks,
                                                    final ScheduledExecutorService executor) {
        log.info("Bootstrapping controller event processors");
        return ControllerEventProcessors.createStreams(localController, config, executor)
                .thenAcceptAsync(x -> streamTransactionMetadataTasks.initializeStreamWriters(localController, config),
                        executor);
    }

    public void handleOrphanedReaders(final Set<String> activeProcesses) {
        log.info("Handling orphaned readers from processes {}", activeProcesses);
        if (this.commitEventEventProcessors != null) {
            handleOrphanedReaders(this.commitEventEventProcessors, activeProcesses);
        }
        if (this.abortEventEventProcessors != null) {
            handleOrphanedReaders(this.abortEventEventProcessors, activeProcesses);
        }
    }

    private void handleOrphanedReaders(final EventProcessorGroup<? extends ControllerEvent> group,
                                       final Set<String> activeProcesses) {
        Set<String> registeredProcesses;
        try {
            registeredProcesses = group.getProcesses();
        } catch (CheckpointStoreException e) {
            log.error(String.format("Error fetching processes registered in event processor group %s", group.toString()), e);
            return;
        }
        registeredProcesses.removeAll(activeProcesses);
        // TODO: remove the following catch NPE once null position objects are handled in ReaderGroup#readerOffline
        for (String process : registeredProcesses) {
            try {
                group.notifyProcessFailure(process);
            } catch (CheckpointStoreException | NullPointerException e) {
                log.error(String.format("Error notifying failure of process=%s in event processor group %s", process,
                        group.toString()), e);
            }
        }
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
                        .supplier(() -> new CommitEventProcessor(streamMetadataStore, hostControllerStore, executor, segmentHelper, connectionFactory))
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
                        .supplier(() -> new AbortEventProcessor(streamMetadataStore, hostControllerStore, executor, segmentHelper, connectionFactory))
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
