/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.ClientFactory;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.util.Retry;
import io.pravega.controller.eventProcessor.ControllerEvent;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroup;
import io.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import io.pravega.controller.eventProcessor.EventProcessorSystem;
import io.pravega.controller.eventProcessor.ExceptionHandler;
import io.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import io.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.stream.Serializer;
import io.pravega.stream.StreamConfiguration;
import io.pravega.stream.impl.ClientFactoryImpl;
import io.pravega.stream.impl.Controller;
import io.pravega.stream.impl.JavaSerializer;
import io.pravega.stream.impl.ReaderGroupManagerImpl;
import io.pravega.stream.impl.netty.ConnectionFactory;
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
    private final Controller controller;
    private final ConnectionFactory connectionFactory;
    private final ClientFactory clientFactory;
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
        this.controller = controller;
        this.connectionFactory = connectionFactory;
        this.clientFactory = new ClientFactoryImpl(config.getScopeName(), controller, connectionFactory);
        this.system = new EventProcessorSystemImpl("Controller", host, config.getScopeName(), clientFactory,
                new ReaderGroupManagerImpl(config.getScopeName(), controller, clientFactory));
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

    private CompletableFuture<Void> createStreams() {
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

        return createScope(config.getScopeName())
                .thenCompose(ignore ->
                        CompletableFuture.allOf(createStream(commitStreamConfig),
                                createStream(abortStreamConfig)));
    }

    private CompletableFuture<Void> createScope(final String scopeName) {
        return FutureHelpers.toVoid(Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating event processor scope " + scopeName, e))
                .runAsync(() -> controller.createScope(scopeName)
                        .thenAccept(x -> log.info("Created controller scope {}", scopeName)), executor));
    }

    private CompletableFuture<Void> createStream(final StreamConfiguration streamConfig) {
        return FutureHelpers.toVoid(Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating event processor stream " + streamConfig.getStreamName(), e))
                .runAsync(() -> controller.createStream(streamConfig)
                        .thenAccept(x ->
                                log.info("Created stream {}/{}", streamConfig.getScope(), streamConfig.getStreamName())),
                        executor));
    }


    public CompletableFuture<Void> bootstrap(final StreamTransactionMetadataTasks streamTransactionMetadataTasks) {
            log.info("Bootstrapping controller event processors");
        return createStreams().thenAcceptAsync(x ->
                streamTransactionMetadataTasks.initializeStreamWriters(clientFactory, config), executor);
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
