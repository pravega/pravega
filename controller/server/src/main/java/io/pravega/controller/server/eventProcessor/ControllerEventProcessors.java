/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.server.eventProcessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.ClientFactory;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.Retry;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroup;
import io.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import io.pravega.controller.eventProcessor.EventProcessorSystem;
import io.pravega.controller.eventProcessor.ExceptionHandler;
import io.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import io.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ScaleEvent;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.Serializer;
import io.pravega.stream.StreamConfiguration;
import io.pravega.stream.impl.ClientFactoryImpl;
import io.pravega.stream.impl.Controller;
import io.pravega.stream.impl.JavaSerializer;
import io.pravega.stream.impl.ReaderGroupManagerImpl;
import io.pravega.stream.impl.netty.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static io.pravega.controller.util.RetryHelper.RETRYABLE_PREDICATE;
import static io.pravega.controller.util.RetryHelper.withRetriesAsync;

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
        this(host, config, controller, checkpointStore, streamMetadataStore, hostControllerStore, segmentHelper, connectionFactory,
                streamMetadataTasks, null, executor);
    }

    @VisibleForTesting
    ControllerEventProcessors(final String host,
                                     final ControllerEventProcessorConfig config,
                                     final Controller controller,
                                     final CheckpointStore checkpointStore,
                                     final StreamMetadataStore streamMetadataStore,
                                     final HostControllerStore hostControllerStore,
                                     final SegmentHelper segmentHelper,
                                     final ConnectionFactory connectionFactory,
                                     final StreamMetadataTasks streamMetadataTasks,
                                     final EventProcessorSystem system,
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
        this.system = system == null ? new EventProcessorSystemImpl("Controller", host, config.getScopeName(), clientFactory,
                new ReaderGroupManagerImpl(config.getScopeName(), controller, clientFactory)) : system;
        this.scaleRequestHandler = new ScaleRequestHandler(streamMetadataTasks, streamMetadataStore, executor);

        this.executor = executor;
    }

    public CompletableFuture<Void> notifyProcessFailure(String process) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        if (commitEventProcessors != null) {
            futures.add(withRetriesAsync(() -> CompletableFuture.runAsync(() -> {
                try {
                    commitEventProcessors.notifyProcessFailure(process);
                } catch (CheckpointStoreException e) {
                    throw new CompletionException(e);
                }
            }, executor), RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor));
        }

        if (abortEventProcessors != null) {
            futures.add(withRetriesAsync(() -> CompletableFuture.runAsync(() -> {
                try {
                    abortEventProcessors.notifyProcessFailure(process);
                } catch (CheckpointStoreException e) {
                    throw new CompletionException(e);
                }
            }, executor), RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor));
        }
        if (scaleEventProcessors != null) {
            futures.add(withRetriesAsync(() -> CompletableFuture.runAsync(() -> {
                try {
                    scaleEventProcessors.notifyProcessFailure(process);
                } catch (CheckpointStoreException e) {
                    throw new CompletionException(e);
                }
            }, executor), RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor));
        }
        return FutureHelpers.allOf(futures);
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

    public CompletableFuture<Void> handleOrphanedReaders(final Supplier<Set<String>> processes) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        if (this.commitEventProcessors != null) {
            futures.add(handleOrphanedReaders(this.commitEventProcessors, processes));
        }
        if (this.abortEventProcessors != null) {
            futures.add(handleOrphanedReaders(this.abortEventProcessors, processes));
        }
        if (this.scaleEventProcessors != null) {
            futures.add(handleOrphanedReaders(this.scaleEventProcessors, processes));
        }
        return FutureHelpers.allOf(futures);
    }

    private CompletableFuture<Void> handleOrphanedReaders(final EventProcessorGroup<? extends ControllerEvent> group,
                                       final Supplier<Set<String>> processes) {
        return withRetriesAsync(() -> CompletableFuture.supplyAsync(() -> {
            try {
                return group.getProcesses();
            } catch (CheckpointStoreException e) {
                if (e.getType().equals(CheckpointStoreException.Type.NoNode)) {
                    return Collections.<String>emptySet();
                }
                throw new CompletionException(e);
            }
        }, executor), RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor)
                .thenComposeAsync(groupProcesses -> withRetriesAsync(() -> CompletableFuture.supplyAsync(() -> {
                    try {
                        return new ImmutablePair<>(processes.get(), groupProcesses);
                    } catch (Exception e) {
                        log.error(String.format("Error fetching current processes%s", group.toString()), e);
                        throw new CompletionException(e);
                    }
                }, executor), RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor))
                .thenComposeAsync(pair -> {
                    Set<String> activeProcesses = pair.getLeft();
                    Set<String> registeredProcesses = pair.getRight();

                    if (registeredProcesses == null || registeredProcesses.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    if (activeProcesses != null) {
                        registeredProcesses.removeAll(activeProcesses);
                    }

                    List<CompletableFuture<Void>> futureList = new ArrayList<>();
                    for (String process : registeredProcesses) {
                        futureList.add(withRetriesAsync(() -> CompletableFuture.runAsync(() -> {
                            try {
                                group.notifyProcessFailure(process);
                            } catch (CheckpointStoreException e) {
                                log.error(String.format("Error notifying failure of process=%s in event processor group %s", process,
                                        group.toString()), e);
                                throw new CompletionException(e);
                            }
                        }, executor), RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor));
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
