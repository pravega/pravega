/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.stream.StreamAlreadyExistsException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Sequence;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.ReaderGroupManagerImpl;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class RequestHandlersInit {
    private static final StreamConfiguration REQUEST_STREAM_CONFIG = StreamConfiguration.builder().scope(Config.INTERNAL_SCOPE).
            streamName(Config.SCALE_STREAM_NAME).scalingPolicy(
            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 1000, 2, 1)).build();

    private static final AtomicReference<ScaleRequestHandler> SCALE_HANDLER_REF = new AtomicReference<>();
    private static final AtomicReference<EventStreamReader<ScaleRequest>> SCALE_READER_REF = new AtomicReference<>();
    private static final AtomicReference<EventStreamWriter<ScaleRequest>> SCALE_WRITER_REF = new AtomicReference<>();
    private static final AtomicReference<RequestReader<ScaleRequest, ScaleRequestHandler>> SCALE_REQUEST_READER_REF = new AtomicReference<>();

    public static void bootstrapRequestHandlers(ControllerService controller, ScheduledExecutorService executor) {
        URI uri = URI.create("tcp://localhost:" + Config.SERVER_PORT);
        ClientFactory clientFactory = new ClientFactoryImpl(Config.INTERNAL_SCOPE, uri);

        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(Config.INTERNAL_SCOPE, uri);

        createScope(controller, executor).thenCompose(x ->
                createStreams(controller, executor)
                        .thenCompose(y -> startScaleReader(clientFactory, readerGroupManager, controller.getStreamMetadataTasks(),
                                controller.getStreamStore(), controller.getStreamTransactionMetadataTasks(),
                                executor)));
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
                            if (res != null && res.equals(CreateScopeStatus.FAILURE)) {
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
                            if (res != null && res.equals(CreateStreamStatus.FAILURE)) {
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

                    readerGroupManager.createReaderGroup(Config.SCALE_READER_GROUP, groupConfig, Lists.newArrayList(Config.SCALE_STREAM_NAME));

                    if (SCALE_HANDLER_REF.get() == null) {
                        SCALE_HANDLER_REF.compareAndSet(null, new ScaleRequestHandler(streamMetadataTasks, streamStore, streamTransactionMetadataTasks, executor));
                    }

                    if (SCALE_READER_REF.get() == null) {
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
                                executor));
                    }

                    CompletableFuture.runAsync(SCALE_REQUEST_READER_REF.get(), Executors.newSingleThreadExecutor());
                    log.debug("bootstrapping request handlers done");
                    result.complete(null);
                    return result;
                }, executor);
        return result;
    }
}
