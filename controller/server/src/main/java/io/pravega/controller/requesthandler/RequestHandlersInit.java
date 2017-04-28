/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.requesthandler;

import io.pravega.ClientFactory;
import io.pravega.ReaderGroupManager;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.Retry;
import io.pravega.shared.controller.requests.ScaleRequest;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.store.stream.DataNotFoundException;
import io.pravega.controller.store.stream.StreamAlreadyExistsException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.shared.NameUtils;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.EventWriterConfig;
import io.pravega.stream.Position;
import io.pravega.stream.ReaderConfig;
import io.pravega.stream.ReaderGroup;
import io.pravega.stream.ReaderGroupConfig;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.Sequence;
import io.pravega.stream.StreamConfiguration;
import io.pravega.stream.impl.JavaSerializer;
import io.pravega.stream.impl.PositionImpl;
import io.pravega.stream.impl.ReaderGroupManagerImpl;
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
                                                                                        .scope(NameUtils.INTERNAL_SCOPE_NAME)
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
        ClientFactory clientFactory = ClientFactory.withScope(NameUtils.INTERNAL_SCOPE_NAME, localController);

        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(NameUtils.INTERNAL_SCOPE_NAME, localController, clientFactory);

        CHECKPOINT_STORE_REF.set(checkpointStore);
        SERIALIZER_REF.set(new JavaSerializer<>());

        CompletableFuture<Void> initFuture = createScope(controller, executor)
                .thenCompose(x -> createStreams(controller, executor))
                .thenCompose(y -> startScaleReader(clientFactory, readerGroupManager, controller.getStreamMetadataTasks(),
                        controller.getStreamStore(), controller.getStreamTransactionMetadataTasks(),
                        executor));
        initFuture.thenComposeAsync(z -> SCALE_REQUEST_READER_REF.get().run(), executor);
        return initFuture;
    }

    public static void shutdownRequestHandlers() throws Exception {
        final RequestReader<ScaleRequest, ScaleRequestHandler> prevHandler = SCALE_REQUEST_READER_REF.getAndSet(null);
        log.info("Closing scale request handler");
        if (prevHandler != null) {
            prevHandler.close();
        }
        final EventStreamReader<ScaleRequest> reader = SCALE_READER_REF.getAndSet(null);
        log.info("Closing scale request stream reader");
        if (reader != null) {
            reader.close();
        }
        final EventStreamWriter<ScaleRequest> writer = SCALE_WRITER_REF.getAndSet(null);
        log.info("Closing scale request stream writer");
        if (writer != null) {
            writer.close();
        }
        log.info("Request handlers shutdown complete");
    }

    private static CompletableFuture<Void> createScope(ControllerService controller, ScheduledExecutorService executor) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Retry.indefinitelyWithExpBackoff(10, 10, 10000,
                e -> log.error("Exception while creating request stream {}", e))
                .runAsync(() -> controller.createScope(NameUtils.INTERNAL_SCOPE_NAME)
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

                    log.info("Bootstrapping request handlers complete");
                    result.complete(null);
                    return result;
                }, executor);
        return result;
    }
}
