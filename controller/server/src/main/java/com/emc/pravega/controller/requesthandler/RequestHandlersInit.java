/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.controller.embedded.EmbeddedController;
import com.emc.pravega.controller.embedded.EmbeddedControllerImpl;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.controller.store.stream.StreamAlreadyExistsException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@Slf4j
public class RequestHandlersInit {
    private static final StreamConfiguration REQUEST_STREAM_CONFIG = StreamConfiguration.builder().scope(Config.INTERNAL_SCOPE).
            streamName(Config.SCALE_STREAM_NAME).scalingPolicy(
            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 1000, 2, 1)).build();

    private static final AtomicReference<ScaleRequestHandler> SCALE_HANDLER_REF = new AtomicReference<>();
    private static final AtomicReference<EventStreamReader<ScaleRequest>> SCALE_READER_REF = new AtomicReference<>();
    private static final AtomicReference<EventStreamWriter<ScaleRequest>> SCALE_WRITER_REF = new AtomicReference<>();
    private static final AtomicReference<RequestReader<ScaleRequest, ScaleRequestHandler>> SCALE_REQUEST_READER_REF = new AtomicReference<>();
    private static ReaderGroupManager readerGroupManager;
    private static ClientFactory clientFactory;
    private static URI uri;

    public static void bootstrapRequestHandlers(EmbeddedController controller, ScheduledExecutorService executor) {

        uri = URI.create("tcp://localhost:" + Config.SERVER_PORT);

        clientFactory = new ClientFactoryImpl(Config.INTERNAL_SCOPE, uri);

        readerGroupManager = new ReaderGroupManagerImpl(Config.INTERNAL_SCOPE, uri);

        EmbeddedControllerImpl embeddedControllerImpl = (EmbeddedControllerImpl) controller;

        CompletableFuture<Void> createStream = new CompletableFuture<>();
        CompletableFuture<Void> createScaleReader = new CompletableFuture<>();

        executor.execute(() -> createStreams(embeddedControllerImpl, executor, createStream));

        createStream.thenAccept(x -> startScaleReader(clientFactory, controller.getController().getStreamMetadataTasks(),
                controller.getController().getStreamStore(), controller.getController().getStreamTransactionMetadataTasks(),
                executor, createScaleReader));
    }

    private static void retryIndefinitely(Runnable supplier, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        try {
            supplier.run();
            result.complete(null);
        } catch (Exception e) {
            // Until we are able to start these readers, keep retrying indefinitely by scheduling it back
            executor.schedule(() -> retryIndefinitely(supplier, executor, result), 10, TimeUnit.SECONDS);
        } catch (Throwable t) {
            log.error("While executing the runnable a throwable was thrown. Stopping retries. {} ", t);
            result.completeExceptionally(t);
        }
    }

    private static <T> void retryIndefinitely(Supplier<CompletableFuture<T>> supplier, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        try {
            supplier.get().whenComplete((r, e) -> {
                if (e != null) {
                    executor.schedule(() -> retryIndefinitely(supplier, executor, result), 10, TimeUnit.SECONDS);
                } else {
                    result.complete(null);
                }
            });
        } catch (Exception e) {
            // Until we are able to start these readers, keep retrying indefinitely by scheduling it back
            executor.schedule(() -> retryIndefinitely(supplier, executor, result), 10, TimeUnit.SECONDS);
        } catch (Throwable t) {
            log.error("While executing the supplier, a throwable was thrown. Stopping retries. {} ", t);
            result.completeExceptionally(t);
        }
    }

    private static void createStreams(EmbeddedControllerImpl controller, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        retryIndefinitely(() -> controller.getController().createStream(REQUEST_STREAM_CONFIG, System.currentTimeMillis())
                .whenComplete((res, ex) -> {
                    if (ex != null && !(ex instanceof StreamAlreadyExistsException)) {
                        // fail and exit
                        throw new CompletionException(ex);
                    }
                    if (res != null && res.equals(CreateStreamStatus.FAILURE)) {
                        throw new RuntimeException("Failed to create stream while starting controller");
                    }
                }), executor, result);
    }

    private static void startScaleReader(ClientFactory clientFactory, StreamMetadataTasks streamMetadataTasks, StreamMetadataStore streamStore, StreamTransactionMetadataTasks streamTransactionMetadataTasks, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        retryIndefinitely(() -> {
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

        }, executor, result);
    }
}
