/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.controller.server.eventProcessor.LocalController;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.store.stream.StreamAlreadyExistsException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.util.Config;
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
import com.emc.pravega.stream.impl.PositionImpl;
import com.emc.pravega.stream.impl.ReaderGroupManagerImpl;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class RequestHandlersInit {
    private static final StreamConfiguration REQUEST_STREAM_CONFIG = StreamConfiguration.builder()
                                                                                        .scope(Config.INTERNAL_SCOPE)
                                                                                        .streamName(Config.SCALE_STREAM_NAME)
                                                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                                                        .build();

    private static final AtomicReference<ScaleRequestHandler> SCALE_HANDLER_REF = new AtomicReference<>();
    private static final AtomicReference<EventStreamReader<ScaleRequest>> SCALE_READER_REF = new AtomicReference<>();
    private static final AtomicReference<EventStreamWriter<ScaleRequest>> SCALE_WRITER_REF = new AtomicReference<>();
    private static final AtomicReference<RequestReader<ScaleRequest, ScaleRequestHandler>> SCALE_REQUEST_READER_REF = new AtomicReference<>();
    private static final AtomicReference<StreamMetadataStore> CHECKPOINT_STORE_REF = new AtomicReference<>();
    private static final AtomicReference<JavaSerializer<Position>> SERIALIZER_REF = new AtomicReference<>();

    public static CompletableFuture<Void> bootstrapRequestHandlers(ControllerService controller,
                                                                   StreamMetadataStore checkpointStore,
                                                                   ScheduledExecutorService executor) {
        Preconditions.checkNotNull(controller);
        Preconditions.checkNotNull(checkpointStore);
        Preconditions.checkNotNull(executor);

        final LocalController localController = new LocalController(controller);
        ClientFactory clientFactory = ClientFactory.withScope(Config.INTERNAL_SCOPE, localController);

        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(Config.INTERNAL_SCOPE, localController, clientFactory);

        CHECKPOINT_STORE_REF.set(checkpointStore);
        SERIALIZER_REF.set(new JavaSerializer<>());

        return createScope(controller, executor)
                .thenCompose(x -> createStreams(controller, executor))
                .thenCompose(y -> startScaleReader(clientFactory, readerGroupManager, controller.getStreamMetadataTasks(),
                        controller.getStreamStore(), controller.getStreamTransactionMetadataTasks(),
                        executor))
                .thenCompose(z -> SCALE_REQUEST_READER_REF.get().run());
    }

    public static void shutdownRequestHandlers() throws Exception {
        final RequestReader<ScaleRequest, ScaleRequestHandler> prevHandler = SCALE_REQUEST_READER_REF.getAndSet(null);
        if (prevHandler != null) {
            prevHandler.close();
        }
    }

    private static CompletableFuture<Void> createScope(ControllerService controller, ScheduledExecutorService executor) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Retry.indefinitelyWithExpBackoff(10, 10, 10000,
                e -> log.error("Exception while creating request stream {}", e))
                .runAsync(() -> controller.createScope(Config.INTERNAL_SCOPE)
                        .whenComplete((res, ex) -> {
                            if (ex != null) {
                                // fail and exit
                                throw new CompletionException(ex);
                            }
                            if (res != null && res.equals(Controller.CreateScopeStatus.Status.FAILURE)) {
                                throw new RuntimeException("Failed to create scope while starting controller");
                            }
                            result.complete(null);
                        }), executor);
        return result;
    }

    private static CompletableFuture<Void> createStreams(ControllerService controller, ScheduledExecutorService executor) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Retry.indefinitelyWithExpBackoff(10, 10, 10000,
                e -> log.error("Exception while creating request stream {}", e))
                .runAsync(() -> controller.createStream(REQUEST_STREAM_CONFIG, System.currentTimeMillis())
                        .whenComplete((res, ex) -> {
                            if (ex != null && !(ex instanceof StreamAlreadyExistsException)) {
                                // fail and exit
                                throw new CompletionException(ex);
                            }
                            if (res != null && res.equals(Controller.CreateStreamStatus.Status.FAILURE)) {
                                throw new RuntimeException("Failed to create stream while starting controller");
                            }
                            result.complete(null);
                        }), executor);
        return result;
    }

    private static CompletableFuture<Void> startScaleReader(ClientFactory clientFactory, ReaderGroupManager readerGroupManager, StreamMetadataTasks streamMetadataTasks, StreamMetadataStore streamStore, StreamTransactionMetadataTasks streamTransactionMetadataTasks, ScheduledExecutorService executor) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Retry.indefinitelyWithExpBackoff(10, 10, 10000,
                e -> log.error("Exception while starting reader {}", e))
                .runAsync(() -> {
                    ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build();

                    ReaderGroup readerGroup = readerGroupManager.createReaderGroup(Config.SCALE_READER_GROUP, groupConfig, Collections.singleton(Config.SCALE_STREAM_NAME));

                    if (SCALE_HANDLER_REF.get() == null) {
                        SCALE_HANDLER_REF.compareAndSet(null, new ScaleRequestHandler(streamMetadataTasks, streamStore, streamTransactionMetadataTasks, executor));
                    }

                    if (SCALE_READER_REF.get() == null) {
                        // If a reader with the same reader id already exists, It means no one reported reader offline.
                        // Report reader offline and create new reader with the same id.
                        if (readerGroup.getOnlineReaders().contains(Config.SCALE_READER_ID)) {
                            // read previous checkpoint if any
                            FutureHelpers.getAndHandleExceptions(CHECKPOINT_STORE_REF.get()
                                    .readCheckpoint(Config.SCALE_READER_GROUP, Config.SCALE_READER_ID)
                                    .handle((res, ex) -> {
                                        if (ex != null && !(ExceptionHelpers.getRealException(ex) instanceof DataNotFoundException)) {
                                            throw new CompletionException(ex);
                                        }

                                        Position position = res == null ? new PositionImpl(Collections.emptyMap()) :
                                                SERIALIZER_REF.get().deserialize(res);
                                        readerGroup.readerOffline(Config.SCALE_READER_ID, position);

                                        return null;
                                    }), CompletionException::new);
                        }

                        SCALE_READER_REF.compareAndSet(null, clientFactory.createReader(Config.SCALE_READER_ID,
                                Config.SCALE_READER_GROUP,
                                new JavaSerializer<>(),
                                ReaderConfig.builder().build()));
                    }

                    if (SCALE_WRITER_REF.get() == null) {
                        SCALE_WRITER_REF.compareAndSet(null, clientFactory.createEventWriter(Config.SCALE_STREAM_NAME,
                                new JavaSerializer<>(),
                                EventWriterConfig.builder().build()));
                    }

                    if (SCALE_REQUEST_READER_REF.get() == null) {
                        SCALE_REQUEST_READER_REF.compareAndSet(null, new RequestReader<>(
                                Config.SCALE_READER_ID,
                                Config.SCALE_READER_GROUP,
                                SCALE_WRITER_REF.get(),
                                SCALE_READER_REF.get(),
                                SCALE_HANDLER_REF.get(),
                                SERIALIZER_REF.get(),
                                CHECKPOINT_STORE_REF.get(),
                                executor));
                    }

                    log.debug("bootstrapping request handlers done");
                    result.complete(null);
                    return result;
                }, executor);
        return result;
    }
}
