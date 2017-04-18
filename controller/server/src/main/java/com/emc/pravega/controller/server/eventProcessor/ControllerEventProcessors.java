/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.eventProcessor.EventProcessorConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.ExceptionHandler;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import com.emc.pravega.controller.requests.ControllerEvent;
import com.emc.pravega.controller.requests.ScaleEvent;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.store.checkpoint.CheckpointStore;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.ReaderGroupManagerImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

@Slf4j
public class ControllerEventProcessors extends AbstractIdleService {

    public static final Serializer<CommitEvent> COMMIT_EVENT_SERIALIZER = new JavaSerializer<>();
    public static final Serializer<AbortEvent> ABORT_EVENT_SERIALIZER = new JavaSerializer<>();
    public static final Serializer<ScaleEvent> SCALE_EVENT_SERIALIZER = new JavaSerializer<>();

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

    private EventProcessorGroup<CommitEvent> commitEventProcessors;
    private EventProcessorGroup<AbortEvent> abortEventProcessors;
    private EventProcessorGroup<ScaleEvent> scaleEventProcessors;
    private ScaleRequestHandler scaleRequestHandler;

    public ControllerEventProcessors(final String host,
                                     final ControllerEventProcessorConfig config,
                                     final Controller controller,
                                     final CheckpointStore checkpointStore,
                                     final StreamMetadataStore streamMetadataStore,
                                     final HostControllerStore hostControllerStore,
                                     final SegmentHelper segmentHelper,
                                     final ConnectionFactory connectionFactory,
                                     final StreamMetadataTasks streamMetadataTasks,
                                     final ScheduledExecutorService executor) {
        this.objectId = "ControllerEventProcessors";
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
        this.scaleRequestHandler = new ScaleRequestHandler(streamMetadataTasks, streamMetadataStore, executor);

        this.executor = executor;
    }

    public void notifyProcessFailure(String process) {
        if (commitEventProcessors != null) {
            CompletableFuture.runAsync(() -> {
                try {
                    commitEventProcessors.notifyProcessFailure(process);
                } catch (CheckpointStoreException e) {
                    throw new CompletionException(e);
                }
            }, executor);
        }
        if (abortEventProcessors != null) {
            CompletableFuture.runAsync(() -> {
                try {
                    abortEventProcessors.notifyProcessFailure(process);
                } catch (CheckpointStoreException e) {
                    throw new CompletionException(e);
                }
            }, executor);
        }
        if (scaleEventProcessors != null) {
            CompletableFuture.runAsync(() -> {
                try {
                    scaleEventProcessors.notifyProcessFailure(process);
                } catch (CheckpointStoreException e) {
                    throw new CompletionException(e);
                }
            }, executor);
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

        StreamConfiguration scaleStreamConfig =
                StreamConfiguration.builder()
                        .scope(config.getScopeName())
                        .streamName(Config.SCALE_STREAM_NAME)
                        .scalingPolicy(ScalingPolicy.fixed(1))
                        .build();

        return createScope(config.getScopeName())
                .thenCompose(ignore ->
                        CompletableFuture.allOf(createStream(commitStreamConfig),
                                createStream(abortStreamConfig),
                                createStream(scaleStreamConfig)));
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

    public void handleOrphanedReaders(final Supplier<Set<String>> processes) {
        if (this.commitEventProcessors != null) {
            CompletableFuture.runAsync(() -> handleOrphanedReaders(this.commitEventProcessors, processes), executor);
        }
        if (this.abortEventProcessors != null) {
            CompletableFuture.runAsync(() -> handleOrphanedReaders(this.abortEventProcessors, processes), executor);
        }
        if (this.scaleEventProcessors != null) {
            CompletableFuture.runAsync(() -> handleOrphanedReaders(this.scaleEventProcessors, processes), executor);
        }
    }

    private void handleOrphanedReaders(final EventProcessorGroup<? extends ControllerEvent> group,
                                       final Supplier<Set<String>> processes) {
        CompletableFuture<Set<String>> future1 = CompletableFuture.supplyAsync(() -> {
            try {
                return group.getProcesses();
            } catch (CheckpointStoreException e) {
                if (e.getType().equals(CheckpointStoreException.Type.NoNode)) {
                    return Collections.emptySet();
                }
                throw new CompletionException(e);
            }
        }, executor);

        CompletableFuture<Set<String>> future2 = CompletableFuture.supplyAsync(() -> {
            try {
                return processes.get();
            } catch (Exception e) {
                log.error(String.format("Error fetching current processes%s", group.toString()), e);
                throw new CompletionException(e);
            }
        }, executor);

        CompletableFuture.allOf(future1, future2)
                .thenCompose((Void v) -> {
                    Set<String> registeredProcesses = FutureHelpers.getAndHandleExceptions(future1, RuntimeException::new);
                    Set<String> activeProcesses = FutureHelpers.getAndHandleExceptions(future2, RuntimeException::new);

                    if (registeredProcesses == null || registeredProcesses.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    if (activeProcesses != null) {
                        registeredProcesses.removeAll(activeProcesses);
                    }

                    List<CompletableFuture<Void>> futureList = new ArrayList<>();
                    for (String process : registeredProcesses) {
                        futureList.add(CompletableFuture.runAsync(() -> {
                            try {
                                group.notifyProcessFailure(process);
                                // TODO: remove the following catch NPE once null position objects are handled in ReaderGroup#readerOffline
                            } catch (CheckpointStoreException | NullPointerException e) {
                                log.error(String.format("Error notifying failure of process=%s in event processor group %s", process,
                                        group.toString()), e);
                                throw new CompletionException(e);
                            }
                        }, executor));
                    }

                    return FutureHelpers.allOf(futureList);
                });
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
                    commitEventProcessors = system.createEventProcessorGroup(commitConfig, checkpointStore);
                    return null;
                });

        // endregion

        // region Create abort event processor

        EventProcessorGroupConfig abortReadersConfig =
                EventProcessorGroupConfigImpl.builder()
                        .streamName(config.getAbortStreamName())
                        .readerGroupName(config.getAbortReaderGroupName())
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
                    abortEventProcessors = system.createEventProcessorGroup(abortConfig, checkpointStore);
                    return null;
                });

        // endregion

        // region Create scale event processor

        EventProcessorGroupConfig scaleReadersConfig =
                EventProcessorGroupConfigImpl.builder()
                        .streamName(config.getScaleStreamName())
                        .readerGroupName(config.getScaleReaderGroupName())
                        .eventProcessorCount(1)
                        .checkpointConfig(config.getScaleCheckpointConfig())
                        .build();

        EventProcessorConfig<ScaleEvent> scaleConfig =
                EventProcessorConfig.<ScaleEvent>builder()
                        .config(scaleReadersConfig)
                        .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                        .serializer(SCALE_EVENT_SERIALIZER)
                        .supplier(() -> new ConcurrentEventProcessor<>(
                                scaleRequestHandler,
                                executor))
                        .build();

        log.info("Creating scale event processors");
        Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating scale event processor group", e))
                .run(() -> {
                    scaleEventProcessors = system.createEventProcessorGroup(scaleConfig, checkpointStore);
                    return null;
                });

        // endregion

        log.info("Awaiting start of commit event processors");
        commitEventProcessors.awaitRunning();
        log.info("Awaiting start of abort event processors");
        abortEventProcessors.awaitRunning();
        log.info("Awaiting start of scale event processors");
        scaleEventProcessors.awaitRunning();
    }

    private void stopEventProcessors() {
        if (commitEventProcessors != null) {
            log.info("Stopping commit event processors");
            commitEventProcessors.stopAsync();
        }
        if (abortEventProcessors != null) {
            log.info("Stopping abort event processors");
            abortEventProcessors.stopAsync();
        }
        if (scaleEventProcessors != null) {
            log.info("Stopping scale event processors");
            scaleEventProcessors.stopAsync();
        }
        if (commitEventProcessors != null) {
            log.info("Awaiting termination of commit event processors");
            commitEventProcessors.awaitTerminated();
        }
        if (abortEventProcessors != null) {
            log.info("Awaiting termination of abort event processors");
            abortEventProcessors.awaitTerminated();
        }
        if (scaleEventProcessors != null) {
            log.info("Awaiting termination of scale event processors");
            scaleEventProcessors.awaitTerminated();
        }
    }
}
