/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.controller.retryable.RetryableException;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.eventProcessor.LocalController;
import com.emc.pravega.controller.store.checkpoint.CheckpointStore;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;
import com.emc.pravega.controller.store.stream.StreamAlreadyExistsException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.shared.NameUtils;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Sequence;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.ReaderGroupManagerImpl;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class RequestHandlers extends AbstractIdleService {
    private static final StreamConfiguration REQUEST_STREAM_CONFIG = StreamConfiguration.builder()
            .scope(NameUtils.INTERNAL_SCOPE_NAME)
            .streamName(Config.SCALE_STREAM_NAME)
            .scalingPolicy(ScalingPolicy.fixed(1))
            .build();

    private final AtomicReference<ScaleRequestHandler> scaleHandlerRef = new AtomicReference<>();
    private final AtomicReference<EventStreamReader<ScaleRequest>> scaleReaderRef = new AtomicReference<>();
    private final AtomicReference<EventStreamWriter<ScaleRequest>> scaleWriterRef = new AtomicReference<>();
    private final AtomicReference<RequestReader<ScaleRequest, ScaleRequestHandler>> scaleRequestReaderRef = new AtomicReference<>();
    private final AtomicReference<ReaderGroupManager> readerGroupManagerRef = new AtomicReference<>();
    private final AtomicReference<ReaderGroupConfig> readerGroupConfigRef = new AtomicReference<>();
    private final AtomicReference<ReaderGroup> readerGroupRef = new AtomicReference<>();

    private final ControllerService controller;
    private final CheckpointStore checkpointStore;
    private final String hostId;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ScheduledExecutorService executor;

    public RequestHandlers(ControllerService controller,
                           CheckpointStore checkpointStore,
                           String hostId,
                           ScheduledExecutorService executor) {
        Preconditions.checkNotNull(controller);
        Preconditions.checkNotNull(checkpointStore);
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(executor);

        this.controller = controller;
        this.checkpointStore = checkpointStore;
        this.hostId = hostId;
        this.executor = executor;
    }

    private CompletableFuture<Void> createScope(ControllerService controller, ScheduledExecutorService executor) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Retry.indefinitelyWithExpBackoff(10, 10, 10000,
                e -> log.error("Exception while creating request stream {}", e))
                .runAsync(() -> {
                    if (shutdown.get()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    return controller.createScope(NameUtils.INTERNAL_SCOPE_NAME)
                            .whenComplete((res, ex) -> {
                                if (ex != null) {
                                    // fail and exit
                                    throw new CompletionException(ex);
                                }
                                if (res != null && res.getStatus().equals(Controller.CreateScopeStatus.Status.FAILURE)) {
                                    throw new RuntimeException("Failed to create scope while starting controller");
                                }
                                result.complete(null);
                            });
                }, executor);
        return result;
    }

    private CompletableFuture<Void> createStreams(ControllerService controller, ScheduledExecutorService executor) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Retry.indefinitelyWithExpBackoff(10, 10, 10000,
                e -> log.error("Exception while creating request stream {}", e))
                .runAsync(() -> {
                    if (shutdown.get()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    return controller.createStream(REQUEST_STREAM_CONFIG, System.currentTimeMillis())
                            .whenComplete((res, ex) -> {
                                if (ex != null && !(ex instanceof StreamAlreadyExistsException)) {
                                    // fail and exit
                                    throw new CompletionException(ex);
                                }
                                if (res != null && res.getStatus().equals(Controller.CreateStreamStatus.Status.FAILURE)) {
                                    throw new RuntimeException("Failed to create stream while starting controller");
                                }
                                result.complete(null);
                            });
                }, executor);
        return result;
    }

    private CompletableFuture<Void> startScaleReader(ClientFactory clientFactory, StreamMetadataTasks streamMetadataTasks, StreamMetadataStore streamStore, StreamTransactionMetadataTasks streamTransactionMetadataTasks, ScheduledExecutorService executor) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Retry.indefinitelyWithExpBackoff(10, 10, 10000,
                e -> log.error("Exception while starting reader {}", e))
                .runAsync(() -> {
                    if (shutdown.get()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    if (readerGroupConfigRef.get() == null) {
                        readerGroupConfigRef.compareAndSet(null,
                                ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build());
                    }
                    if (readerGroupRef.get() == null) {
                        readerGroupRef.compareAndSet(null, readerGroupManagerRef.get()
                                .createReaderGroup(Config.SCALE_READER_GROUP, readerGroupConfigRef.get(),
                                        Collections.singleton(Config.SCALE_STREAM_NAME)));
                    }
                    if (scaleHandlerRef.get() == null) {
                        scaleHandlerRef.compareAndSet(null,
                                new ScaleRequestHandler(streamMetadataTasks, streamStore, streamTransactionMetadataTasks, executor));
                    }

                    try {
                        checkpointStore.addReaderGroup(hostId, Config.SCALE_READER_GROUP);
                    } catch (CheckpointStoreException e) {
                        if (!e.getType().equals(CheckpointStoreException.Type.NodeExists)) {
                            log.warn("error creating readergroup in checkpoint store {}", e);
                            throw new CompletionException(e);
                        }
                    }

                    try {
                        checkpointStore.addReader(hostId, Config.SCALE_READER_GROUP, Config.SCALE_READER_ID);
                    } catch (CheckpointStoreException e) {
                        if (!e.getType().equals(CheckpointStoreException.Type.NodeExists)) {
                            log.warn("error creating reader in checkpoint store {}", e);
                            throw new CompletionException(e);
                        }
                    }

                    if (scaleReaderRef.get() == null) {
                        // If a reader with the same reader id already exists, It means no one reported reader offline.
                        // Report reader offline and create new reader with the same id.
                        assert !readerGroupRef.get().getOnlineReaders().contains(getReaderId(hostId, Config.SCALE_READER_ID));

                        // add this to checkpoint store before creating the reader
                        scaleReaderRef.compareAndSet(null, clientFactory.createReader(getReaderId(hostId, Config.SCALE_READER_ID),
                                Config.SCALE_READER_GROUP,
                                new JavaSerializer<>(),
                                ReaderConfig.builder().build()));
                    }

                    if (scaleWriterRef.get() == null) {
                        scaleWriterRef.compareAndSet(null, clientFactory.createEventWriter(Config.SCALE_STREAM_NAME,
                                new JavaSerializer<>(),
                                EventWriterConfig.builder().build()));
                    }

                    if (scaleRequestReaderRef.get() == null) {
                        scaleRequestReaderRef.compareAndSet(null, new RequestReader<>(
                                scaleWriterRef.get(),
                                scaleReaderRef.get(),
                                scaleHandlerRef.get(),
                                (Position position) -> checkpointStore.setPosition(hostId, Config.SCALE_READER_GROUP, Config.SCALE_READER_ID, position),
                                executor));
                    }

                    log.info("Bootstrapping request handlers complete");
                    result.complete(null);
                    return result;
                }, executor);
        return result;
    }

    private String getReaderId(String hostId, String readerId) {
        return hostId + readerId;
    }

    @Override
    protected void startUp() throws Exception {
        final LocalController localController = new LocalController(controller);
        ClientFactory clientFactory = ClientFactory.withScope(NameUtils.INTERNAL_SCOPE_NAME, localController);

        readerGroupManagerRef.set(new ReaderGroupManagerImpl(NameUtils.INTERNAL_SCOPE_NAME, localController, clientFactory));

        CompletableFuture<Void> future = createScope(controller, executor)
                .thenCompose(x -> createStreams(controller, executor))
                .thenCompose(y -> startScaleReader(clientFactory, controller.getStreamMetadataTasks(),
                        controller.getStreamStore(), controller.getStreamTransactionMetadataTasks(),
                        executor));
        future.thenComposeAsync(z -> scaleRequestReaderRef.get().run(), executor);
        future.get();
    }

    @Override
    protected void shutDown() throws Exception {
        shutdown.set(true);
        awaitRunning();
        final RequestReader<ScaleRequest, ScaleRequestHandler> requestReader = scaleRequestReaderRef.getAndSet(null);
        log.info("Closing scale request handler");
        if (requestReader != null) {
            requestReader.close();
        }
        final EventStreamReader<ScaleRequest> reader = scaleReaderRef.getAndSet(null);
        log.info("Closing scale request stream reader");
        if (reader != null) {
            reader.close();
        }
        final EventStreamWriter<ScaleRequest> writer = scaleWriterRef.getAndSet(null);
        log.info("Closing scale request stream writer");
        if (writer != null) {
            writer.close();
        }

        try {
            notifyProcessFailure(getReaderId(hostId, Config.SCALE_READER_ID)).get();
        } catch (Exception e) {
            log.error("Error trying to mark this reader as offline, so no graceful shutdown of reader: {}", e);
        }
        if (readerGroupManagerRef.get() != null) {
            readerGroupManagerRef.getAndSet(null).close();
        }

        readerGroupConfigRef.getAndSet(null);
        readerGroupRef.getAndSet(null);

        log.info("Request handlers shutdown complete");
    }

    public CompletableFuture<Void> notifyProcessFailure(String hostId) {
        return Retry.withExpBackoff(100, 2, 1000, 10000)
                .retryWhen(RetryableException::isRetryable)
                .throwingOn(Exception.class)
                .runAsync(() -> {
                    Map<String, Position> readers;
                    try {
                        readers = checkpointStore.getPositions(hostId, Config.SCALE_READER_GROUP);
                    } catch (CheckpointStoreException e) {
                        if (!e.getType().equals(CheckpointStoreException.Type.NoNode)) {
                            throw new CompletionException(e);
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    }

                    readers.entrySet().forEach(reader -> {
                        String readerId = getReaderId(hostId, reader.getKey());

                        if (readerGroupRef.get() != null && readerGroupRef.get().getOnlineReaders().contains(readerId)) {
                            readerGroupRef.get().readerOffline(readerId, reader.getValue());
                        }
                    });

                    return CompletableFuture.completedFuture(null);
                }, executor);
    }

    public CompletableFuture<Void> sweepOrphanedReaders(Supplier<Set<String>> runningProcesses) {
        return Retry.indefinitelyWithExpBackoff(10, 2, 10000,
                e -> log.warn("sweeping orphaned readers threw exception{}", e))
                .runAsync(() -> {
                    Set<String> allInStore;
                    try {
                        allInStore = checkpointStore.getProcesses();
                        allInStore.removeAll(runningProcesses.get());

                    } catch (Exception e) {
                        log.debug("Checkpoint store get process threw exception: {}", e);
                        throw new CompletionException(e);
                    }

                    return FutureHelpers.allOf(allInStore.stream().map(this::notifyProcessFailure).collect(Collectors.toList()));
                }, executor);
    }
}
