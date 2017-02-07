/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;
import com.emc.pravega.common.concurrent.FutureHelpers;
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
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.StreamManagerImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@Slf4j
public class RequestHandlersInit {
    private static final StreamConfiguration REQUEST_STREAM_CONFIG = new StreamConfigurationImpl(Config.INTERNAL_SCOPE,
            Config.SCALE_STREAM_NAME,
            new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_EVENTS, 1000, 2, 1));

    private static AtomicReference<ScaleRequestHandler> scaleHandler = new AtomicReference<>();
    private static AtomicReference<EventStreamReader<ScaleRequest>> scaleReader = new AtomicReference<>();
    private static AtomicReference<EventStreamWriter<ScaleRequest>> scaleWriter = new AtomicReference<>();
    private static AtomicReference<RequestReader<ScaleRequest, ScaleRequestHandler>> scaleRequestReader = new AtomicReference<>();
    private static StreamManager streamManager;
    private static ClientFactory clientFactory;

    public static void coldStart(EmbeddedController controller, ScheduledExecutorService executor) {

        clientFactory = new ClientFactoryImpl(Config.INTERNAL_SCOPE, controller, new ConnectionFactoryImpl(false));

        streamManager = new StreamManagerImpl(Config.INTERNAL_SCOPE, controller, clientFactory);

        EmbeddedControllerImpl embeddedControllerImpl = (EmbeddedControllerImpl) controller;

        CompletableFuture<Void> createStream = new CompletableFuture<>();
        CompletableFuture<Void> createScaleReader = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> createStreams(embeddedControllerImpl, executor, createStream));

        createStream.thenAccept(x -> startScaleReader(clientFactory, controller.getController().getStreamMetadataTasks(),
                controller.getController().getStreamStore(), controller.getController().getStreamTransactionMetadataTasks(),
                executor, createScaleReader));
    }

    private static void retryIndefinitely(Supplier<Void> supplier, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        try {
            supplier.get();
            result.complete(null);
        } catch (Exception e) {
            // Until we are able to start these readers, keep retrying indefinitely by scheduling it back
            executor.schedule(() -> retryIndefinitely(supplier, executor, result), 10, TimeUnit.SECONDS);
        }
    }

    private static void createStreams(EmbeddedControllerImpl controller, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        retryIndefinitely(() -> {

            CompletableFuture<CreateStreamStatus> requestStreamFuture = streamCreationCompletionCallback(
                    controller.getController().createStream(REQUEST_STREAM_CONFIG, System.currentTimeMillis()));

            FutureHelpers.getAndHandleExceptions(requestStreamFuture, RuntimeException::new);
            return null;
        }, executor, result);
    }

    private static void startScaleReader(ClientFactory clientFactory, StreamMetadataTasks streamMetadataTasks, StreamMetadataStore streamStore, StreamTransactionMetadataTasks streamTransactionMetadataTasks, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        retryIndefinitely(() -> {
            // TODO: what should be starting position? to be read from checkpoint?
            ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build();

            streamManager.createReaderGroup(Config.SCALE_READER_GROUP, groupConfig, Lists.newArrayList(Config.SCALE_STREAM_NAME));

            if (scaleHandler.get() == null) {
                scaleHandler.compareAndSet(null, new ScaleRequestHandler(streamMetadataTasks, streamStore, streamTransactionMetadataTasks));
            }

            if (scaleReader.get() == null) {
                scaleReader.compareAndSet(null, clientFactory.createReader(Config.SCALE_READER_ID,
                        Config.SCALE_READER_GROUP,
                        new JavaSerializer<>(),
                        new ReaderConfig()));
            }

            if (scaleWriter.get() == null) {
                scaleWriter.compareAndSet(null, clientFactory.createEventWriter(Config.SCALE_STREAM_NAME,
                        new JavaSerializer<>(),
                        new EventWriterConfig(null)));
            }

            if (scaleRequestReader.get() == null) {
                scaleRequestReader.compareAndSet(null, new RequestReader<>(
                        Config.SCALE_READER_ID,
                        Config.SCALE_READER_GROUP,
                        scaleWriter.get(),
                        scaleReader.get(),
                        streamStore,
                        scaleHandler.get(),
                        executor));
            }

            CompletableFuture.runAsync(scaleRequestReader.get(), Executors.newSingleThreadExecutor());
            return null;
        }, executor, result);
    }

    private static CompletableFuture<CreateStreamStatus> streamCreationCompletionCallback(CompletableFuture<CreateStreamStatus> createFuture) {
        return createFuture.whenComplete((res, ex) -> {
            if (ex != null && !(ex instanceof StreamAlreadyExistsException)) {
                // fail and exit
                throw new RuntimeException(ex);
            }
            if (res != null && res.equals(CreateStreamStatus.FAILURE)) {
                throw new RuntimeException("Failed to create stream while starting controller");
            }
        });
    }
}
