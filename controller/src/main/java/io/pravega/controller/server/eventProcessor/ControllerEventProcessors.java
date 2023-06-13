/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.server.eventProcessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroup;
import io.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import io.pravega.controller.eventProcessor.EventProcessorSystem;
import io.pravega.controller.eventProcessor.EventSerializer;
import io.pravega.controller.eventProcessor.ExceptionHandler;
import io.pravega.controller.eventProcessor.impl.ConcurrentEventProcessor;
import io.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import io.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import io.pravega.controller.fault.FailoverSweeper;
import io.pravega.controller.server.eventProcessor.requesthandlers.AbortRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.CommitRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.CreateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteScopeTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.TruncateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.CreateTableTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.DeleteTableTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.TableRequestHandler;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.util.RetryHelper.RETRYABLE_PREDICATE;
import static io.pravega.controller.util.RetryHelper.withRetriesAsync;

@Slf4j
public class ControllerEventProcessors extends AbstractIdleService implements FailoverSweeper, AutoCloseable {

    public static final EventSerializer<CommitEvent> COMMIT_EVENT_SERIALIZER = new EventSerializer<>();
    public static final EventSerializer<AbortEvent> ABORT_EVENT_SERIALIZER = new EventSerializer<>();
    public static final EventSerializer<ControllerEvent> CONTROLLER_EVENT_SERIALIZER = new EventSerializer<>();

    // Retry configuration
    private static final long DELAY = 100;
    private static final int MULTIPLIER = 10;
    private static final long MAX_DELAY = 10000;
    private static final long TRUNCATION_INTERVAL_MILLIS = Duration.ofMinutes(10).toMillis();

    private final String objectId;
    private final ControllerEventProcessorConfig config;
    private final CheckpointStore checkpointStore;
    private final EventProcessorSystem system;
    private final ClientFactoryImpl clientFactory;
    private final ScheduledExecutorService executor;

    private EventProcessorGroup<CommitEvent> commitEventProcessors;
    private EventProcessorGroup<AbortEvent> abortEventProcessors;
    private EventProcessorGroup<ControllerEvent> requestEventProcessors;
    private EventProcessorGroup<ControllerEvent> kvtRequestEventProcessors;
    private final StreamRequestHandler streamRequestHandler;
    private final CommitRequestHandler commitRequestHandler;
    private final AbortRequestHandler abortRequestHandler;
    private final TableRequestHandler kvtRequestHandler;
    private final long rebalanceIntervalMillis;
    private final AtomicLong truncationInterval;
    private ScheduledExecutorService rebalanceExecutor;
    private final LocalController controller;
    @Getter
    private final AtomicBoolean bootstrapCompleted = new AtomicBoolean(false);

    public ControllerEventProcessors(final String host,
                                     final ControllerEventProcessorConfig config,
                                     final LocalController controller,
                                     final CheckpointStore checkpointStore,
                                     final StreamMetadataStore streamMetadataStore,
                                     final BucketStore bucketStore,
                                     final ConnectionPool connectionPool,
                                     final StreamMetadataTasks streamMetadataTasks,
                                     final StreamTransactionMetadataTasks streamTransactionMetadataTasks,
                                     final KVTableMetadataStore kvtMetadataStore,
                                     final TableMetadataTasks kvtMetadataTasks,
                                     final ScheduledExecutorService executor) {
        this(host, config, controller, checkpointStore, streamMetadataStore, bucketStore, connectionPool,
                streamMetadataTasks, streamTransactionMetadataTasks,  kvtMetadataStore, kvtMetadataTasks, null, executor);
    }

    @VisibleForTesting
    public ControllerEventProcessors(final String host,
                                     final ControllerEventProcessorConfig config,
                                     final LocalController controller,
                                     final CheckpointStore checkpointStore,
                                     final StreamMetadataStore streamMetadataStore,
                                     final BucketStore bucketStore,
                                     final ConnectionPool connectionPool,
                                     final StreamMetadataTasks streamMetadataTasks,
                                     final StreamTransactionMetadataTasks streamTransactionMetadataTasks,
                                     final KVTableMetadataStore kvtMetadataStore,
                                     final TableMetadataTasks kvtMetadataTasks,
                                     final EventProcessorSystem system,
                                     final ScheduledExecutorService executor) {
        this.objectId = "ControllerEventProcessors";
        this.config = config;
        this.checkpointStore = checkpointStore;
        this.controller = controller;
        this.clientFactory = new ClientFactoryImpl(config.getScopeName(), controller, connectionPool);
        this.system = system == null ? new EventProcessorSystemImpl("Controller", host, config.getScopeName(), clientFactory,
                new ReaderGroupManagerImpl(config.getScopeName(), controller, clientFactory)) : system;
        this.streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamMetadataStore, executor),
                new ScaleOperationTask(streamMetadataTasks, streamMetadataStore, executor),
                new UpdateStreamTask(streamMetadataTasks, streamMetadataStore, bucketStore, executor),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamMetadataStore, executor),
                new DeleteStreamTask(streamMetadataTasks, streamMetadataStore, bucketStore, executor),
                new TruncateStreamTask(streamMetadataTasks, streamMetadataStore, executor),
                new CreateReaderGroupTask(streamMetadataTasks, streamMetadataStore, executor),
                new DeleteReaderGroupTask(streamMetadataTasks, streamMetadataStore, executor),
                new UpdateReaderGroupTask(streamMetadataTasks, streamMetadataStore, executor),
                streamMetadataStore,
                new DeleteScopeTask(streamMetadataTasks, streamMetadataStore, kvtMetadataStore, kvtMetadataTasks, executor),
                executor);
        this.commitRequestHandler = new CommitRequestHandler(streamMetadataStore, streamMetadataTasks, 
                streamTransactionMetadataTasks, bucketStore, executor);
        this.abortRequestHandler = new AbortRequestHandler(streamMetadataStore, streamMetadataTasks, executor);
        this.kvtRequestHandler = new TableRequestHandler(new CreateTableTask(kvtMetadataStore, kvtMetadataTasks, executor),
                                                            new DeleteTableTask(kvtMetadataStore, kvtMetadataTasks, executor),
                                                            kvtMetadataStore, executor);
        this.executor = executor;
        this.rebalanceIntervalMillis = config.getRebalanceIntervalMillis();
        this.truncationInterval = new AtomicLong(TRUNCATION_INTERVAL_MILLIS);
    }

    /**
     * Get the health status.
     *
     * @return true if zookeeper is connected.
     */
    public boolean isMetadataServiceConnected() {
        return  checkpointStore.isHealthy();
     }

    /**
     * Get bootstrap completed status.
     *
     * @return true if bootstrapCompleted is set to true.
     */
    public boolean isBootstrapCompleted() {
        return this.bootstrapCompleted.get();
    }

    /**
     * Get the health status.
     *
     * @return true if zookeeper is connected and bootstrap is completed.
     */
    @Override
    public boolean isReady() {
        boolean isMetaConnected = isMetadataServiceConnected();
        boolean isBootstrapComplete = isBootstrapCompleted();
        boolean isSvcRunning = this.isRunning();
        boolean isReady = isMetaConnected && isBootstrapComplete && isSvcRunning;
        log.debug("IsReady={} as isMetaConnected={}, isBootstrapComplete={}, isSvcRunning={}", isReady, isMetaConnected, isBootstrapComplete, isSvcRunning);
        return isReady;
    }

    @Override
    protected void startUp() throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startUp");
        try {
            log.info("Starting controller event processors");
            rebalanceExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "event-processor");
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
            rebalanceExecutor.shutdownNow();
            this.clientFactory.close();
            log.info("Controller event processors shutDown complete");
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
        }
    }

     @Override
    public CompletableFuture<Void> sweepFailedProcesses(final Supplier<Set<String>> processes) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        if (this.commitEventProcessors != null) {
            futures.add(handleOrphanedReaders(this.commitEventProcessors, processes));
        }
        if (this.abortEventProcessors != null) {
            futures.add(handleOrphanedReaders(this.abortEventProcessors, processes));
        }
        if (this.requestEventProcessors != null) {
            futures.add(handleOrphanedReaders(this.requestEventProcessors, processes));
        }
        if (this.kvtRequestEventProcessors != null) {
            futures.add(handleOrphanedReaders(this.kvtRequestEventProcessors, processes));
        }
        return Futures.allOf(futures);
    }

    @Override
    public CompletableFuture<Void> handleFailedProcess(String process) {
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
        if (requestEventProcessors != null) {
            futures.add(withRetriesAsync(() -> CompletableFuture.runAsync(() -> {
                try {
                    requestEventProcessors.notifyProcessFailure(process);
                } catch (CheckpointStoreException e) {
                    throw new CompletionException(e);
                }
            }, executor), RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor));
        }
        if (kvtRequestEventProcessors != null) {
            futures.add(withRetriesAsync(() -> CompletableFuture.runAsync(() -> {
                try {
                    kvtRequestEventProcessors.notifyProcessFailure(process);
                } catch (CheckpointStoreException e) {
                    throw new CompletionException(e);
                }
            }, executor), RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor));
        }
        return Futures.allOf(futures);
    }

    private CompletableFuture<Void> createStreams() {
        StreamConfiguration commitStreamConfig = StreamConfiguration.builder()
                                                                    .scalingPolicy(config.getCommitStreamScalingPolicy())
                                                                    .build();

        StreamConfiguration abortStreamConfig = StreamConfiguration.builder()
                                                                   .scalingPolicy(config.getAbortStreamScalingPolicy())
                                                                   .build();

        StreamConfiguration requestStreamConfig = StreamConfiguration.builder()
                                                                     .scalingPolicy(config.getRequestStreamScalingPolicy())
                                                                     .build();
        StreamConfiguration kvTableStreamConfig = StreamConfiguration.builder()
                                                                            .scalingPolicy(config.getKvtStreamScalingPolicy())
                                                                            .build();

        String scope = config.getScopeName();
        CompletableFuture<Void> future = createScope(scope);
        return future.thenCompose(ignore -> CompletableFuture.allOf(createStream(scope, config.getCommitStreamName(),
                                                                                 commitStreamConfig),
                                                                    createStream(scope, config.getAbortStreamName(),
                                                                                 abortStreamConfig),
                                                                    createStream(scope, Config.SCALE_STREAM_NAME,
                                                                                 requestStreamConfig),
                                                                    createStream(scope, config.getKvtStreamName(),
                                                                                 kvTableStreamConfig)));
    }

    private CompletableFuture<Void> createScope(final String scopeName) {
        return Futures.toVoid(Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating event processor scope {} with exception {}", scopeName, 
                        Exceptions.unwrap(e).toString()))
                                   .runAsync(() -> controller.createScope(scopeName)
                        .thenAccept(x -> log.info("Created internal scope {}", scopeName)), executor));
    }

    private CompletableFuture<Void> createStream(String scope, String streamName, final StreamConfiguration streamConfig) {
        return Futures.toVoid(Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating event processor stream {} with exception {}", streamName, 
                        Exceptions.unwrap(e).toString()))
                                   .runAsync(() -> controller.createInternalStream(scope, streamName, streamConfig)
                                .thenAccept(x ->
                                        log.info("Created internal stream {}/{}", scope, streamName)),
                        executor));
    }

    public CompletableFuture<Void> bootstrap(final StreamTransactionMetadataTasks streamTransactionMetadataTasks,
                                             StreamMetadataTasks streamMetadataTasks, TableMetadataTasks tableMetadataTasks) {
        log.info("Bootstrapping controller event processors");
        return createStreams().thenAcceptAsync(x -> {
            streamMetadataTasks.initializeStreamWriters(clientFactory, config.getRequestStreamName());
            streamTransactionMetadataTasks.initializeStreamWriters(clientFactory, config);
            tableMetadataTasks.initializeStreamWriters(clientFactory, config.getKvtStreamName());

            long delay = truncationInterval.get();
            Futures.loop(this::isRunning, () -> Futures.delayedFuture(
                    () -> truncate(config.getRequestStreamName(), config.getRequestReaderGroupName(), streamMetadataTasks),
                    delay, executor), executor);
            Futures.loop(this::isRunning, () -> Futures.delayedFuture(
                    () -> truncate(config.getCommitStreamName(), config.getCommitReaderGroupName(), streamMetadataTasks),
                    delay, executor), executor);
            Futures.loop(this::isRunning, () -> Futures.delayedFuture(
                    () -> truncate(config.getAbortStreamName(), config.getAbortReaderGroupName(), streamMetadataTasks), 
                    delay, executor), executor);
            Futures.loop(this::isRunning, () -> Futures.delayedFuture(
                    () -> truncate(config.getKvtStreamName(), config.getKvtReaderGroupName(), streamMetadataTasks),
                    delay, executor), executor);
            this.bootstrapCompleted.set(true);
            log.info("Completed bootstrapping event processors.");
        }, executor);
    }

    @VisibleForTesting
    CompletableFuture<Void> truncate(String streamName, String readergroupName, StreamMetadataTasks streamMetadataTasks) {
        Preconditions.checkState(isRunning());
        try {
            // 1. get all processes from checkpoint store 
            // 2. get the all checkpoints for all readers in the readergroup for the stream.
            // 3. consolidate all checkpoints to create a stream cut. 
            // 4. submit a truncation job for the stream
            Map<String, Position> positions = new HashMap<>();
            for (String process : checkpointStore.getProcesses()) {
                positions.putAll(checkpointStore.getPositions(process, readergroupName));
            }
            Map<Long, Long> streamcut = positions
                    .entrySet().stream().map(x -> x.getValue() == null ? Collections.<Long, Long>emptyMap() : convertPosition(x))
                    .reduce(Collections.emptyMap(), (x, y) -> {
                        Map<Long, Long> result = new HashMap<>(x);
                        y.forEach((a, b) -> {
                            if (x.containsKey(a)) {
                                result.put(a, Math.max(x.get(a), b));
                            } else {
                                result.put(a, b);
                            }
                        });
                        return result;
                    });
            // Start a truncation job, but handle its exception cases by simply logging it. We will not fail the future
            // so that the loop can continue in the next iteration and attempt to truncate the stream. 
            return streamMetadataTasks.startTruncation(config.getScopeName(), streamName, streamcut, null)
                      .handle((r, e) -> {
                          if (e != null) {
                              log.warn("Submission for truncation for stream {} failed. Will be retried in next iteration.",
                                      streamName);
                          } else if (r) {
                              log.debug("Truncation for stream {} at streamcut {} submitted.",
                                      streamName, streamcut);
                          } else {
                              log.debug("Truncation for stream {} at streamcut {} rejected.",
                                      streamName, streamcut);
                          }
                          return null;
                      });
        } catch (Exception e) {
            // we will catch and log all exceptions and return a completed future so that the truncation is attempted in the
            // next iteration
            Throwable unwrap = Exceptions.unwrap(e);
            log.warn("Encountered exception attempting to truncate stream {}. {}: {}", streamName, unwrap.getClass().getName(), 
                    unwrap.getMessage());
            return CompletableFuture.completedFuture(null);
        }
    }

    private Map<Long, Long> convertPosition(Map.Entry<String, Position> x) {
        return ((PositionImpl) x.getValue().asImpl()).getOwnedSegmentsWithOffsets().entrySet().stream()
                                                     .collect(Collectors.toMap(y -> y.getKey().getSegmentId(), Map.Entry::getValue));
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
                                log.error(String.format("Error notifying failure of process=%s in event processor group %s", 
                                        process, group.toString()), e);
                                throw new CompletionException(e);
                            }
                        }, executor), RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor));
                    }

                    return Futures.allOf(futureList);
                });
    }

    private void initialize() {
        // region Create commit event processor
        EventProcessorGroupConfig commitReadersConfig =
                EventProcessorGroupConfigImpl.builder()
                        .streamName(config.getCommitStreamName())
                        .readerGroupName(config.getCommitReaderGroupName())
                        .eventProcessorCount(config.getCommitReaderGroupSize())
                        .checkpointConfig(CheckpointConfig.none())
                        .build();

        EventProcessorConfig<CommitEvent> commitConfig =
                EventProcessorConfig.<CommitEvent>builder()
                        .config(commitReadersConfig)
                        .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                        .serializer(COMMIT_EVENT_SERIALIZER)
                        .supplier(() -> new ConcurrentEventProcessor<>(commitRequestHandler, executor))
                        .minRebalanceIntervalMillis(rebalanceIntervalMillis)
                        .build();

        log.debug("Creating commit event processors");
        Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating commit event processor group", e))
                .run(() -> {
                    commitEventProcessors = system.createEventProcessorGroup(commitConfig, checkpointStore, rebalanceExecutor);
                    return null;
                });

        // endregion

        // region Create abort event processor

        EventProcessorGroupConfig abortReadersConfig =
                EventProcessorGroupConfigImpl.builder()
                        .streamName(config.getAbortStreamName())
                        .readerGroupName(config.getAbortReaderGroupName())
                        .eventProcessorCount(config.getAbortReaderGroupSize())
                        .checkpointConfig(CheckpointConfig.none())
                        .build();

        EventProcessorConfig<AbortEvent> abortConfig =
                EventProcessorConfig.<AbortEvent>builder()
                        .config(abortReadersConfig)
                        .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                        .serializer(ABORT_EVENT_SERIALIZER)
                        .supplier(() -> new ConcurrentEventProcessor<>(abortRequestHandler, executor))
                        .minRebalanceIntervalMillis(rebalanceIntervalMillis)
                        .build();

        log.debug("Creating abort event processors");
        Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating abort event processor group", e))
                .run(() -> {
                    abortEventProcessors = system.createEventProcessorGroup(abortConfig, checkpointStore, rebalanceExecutor);
                    return null;
                });

        // endregion

        // region Create request event processor
        EventProcessorGroupConfig requestReadersConfig =
                EventProcessorGroupConfigImpl.builder()
                        .streamName(config.getRequestStreamName())
                        .readerGroupName(config.getRequestReaderGroupName())
                        .eventProcessorCount(1)
                        .checkpointConfig(CheckpointConfig.none())
                        .build();

        EventProcessorConfig<ControllerEvent> requestConfig =
                EventProcessorConfig.builder()
                        .config(requestReadersConfig)
                        .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                        .serializer(CONTROLLER_EVENT_SERIALIZER)
                        .supplier(() -> new ConcurrentEventProcessor<>(streamRequestHandler, executor))
                        .minRebalanceIntervalMillis(rebalanceIntervalMillis)
                        .build();

        log.debug("Creating stream request event processors");
        Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating request event processor group", e))
                .run(() -> {
                    requestEventProcessors = system.createEventProcessorGroup(requestConfig, checkpointStore, rebalanceExecutor);
                    return null;
                });

        // endregion

        // region Create KVtable event processor
        EventProcessorGroupConfig kvtReadersConfig =
                EventProcessorGroupConfigImpl.builder()
                        .streamName(config.getKvtStreamName())
                        .readerGroupName(config.getKvtReaderGroupName())
                        .eventProcessorCount(1)
                        .checkpointConfig(CheckpointConfig.none())
                        .build();

        EventProcessorConfig<ControllerEvent> kvtRequestConfig =
                EventProcessorConfig.builder()
                        .config(kvtReadersConfig)
                        .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                        .serializer(CONTROLLER_EVENT_SERIALIZER)
                        .supplier(() -> new ConcurrentEventProcessor<>(kvtRequestHandler, executor))
                        .minRebalanceIntervalMillis(rebalanceIntervalMillis)
                        .build();

        log.debug("Creating kvt event processors");
        Retry.indefinitelyWithExpBackoff(DELAY, MULTIPLIER, MAX_DELAY,
                e -> log.warn("Error creating kvt event processor group", e))
                .run(() -> {
                    kvtRequestEventProcessors = system.createEventProcessorGroup(kvtRequestConfig, checkpointStore, rebalanceExecutor);
                    return null;
                });

        // endregion
        log.info("Awaiting start of event processors...");
        commitEventProcessors.awaitRunning();
        log.info("Commit event processor started.");
        abortEventProcessors.awaitRunning();
        log.info("Abort event processor started.");
        requestEventProcessors.awaitRunning();
        log.info("Stream request event processor started.");
        kvtRequestEventProcessors.awaitRunning();
        log.info("KVT request event processor started.");
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
        if (requestEventProcessors != null) {
            log.info("Stopping request event processors");
            requestEventProcessors.stopAsync();
        }
        if (kvtRequestEventProcessors != null) {
            log.info("Stopping kvt request event processors");
            kvtRequestEventProcessors.stopAsync();
        }

        if (commitEventProcessors != null) {
            log.info("Awaiting termination of commit event processors");
            commitEventProcessors.awaitTerminated();
        }
        if (abortEventProcessors != null) {
            log.info("Awaiting termination of abort event processors");
            abortEventProcessors.awaitTerminated();
        }
        if (requestEventProcessors != null) {
            log.info("Awaiting termination of request event processors");
            requestEventProcessors.awaitTerminated();
        }
        if (kvtRequestEventProcessors != null) {
            log.info("Awaiting termination of kvt request event processors");
            kvtRequestEventProcessors.awaitTerminated();
        }
    }
    
    @VisibleForTesting
    void setTruncationInterval(long interval) {
        truncationInterval.set(interval);
    }

    @Override
    public void close() {
        this.clientFactory.close();
        try {
            this.stopAsync().awaitTerminated(config.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            log.error("Timeout expired while waiting for service to shut down.", ex);
        }
    }
}
