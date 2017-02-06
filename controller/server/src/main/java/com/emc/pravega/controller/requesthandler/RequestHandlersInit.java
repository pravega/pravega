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
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.controller.requests.TxTimeoutRequest;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
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
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.PositionImpl;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@Slf4j
public class RequestHandlersInit {
    @VisibleForTesting
    public static final StreamConfiguration REQUEST_STREAM_CONFIG = new StreamConfigurationImpl(Config.INTERNAL_SCOPE,
            Config.SCALE_STREAM_NAME,
            new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_EVENTS, 1000, 2, 1));

    @VisibleForTesting
    public static final StreamConfiguration TXN_TIMER_STREAM_CONFIG = new StreamConfigurationImpl(Config.INTERNAL_SCOPE,
            Config.TXN_TIMER_STREAM_NAME,
            new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_EVENTS, 1000, 2, 1));

    private static AtomicReference<TransactionTimer> txnHandler = new AtomicReference<>();
    private static AtomicReference<RequestReader> txnRequestReader = new AtomicReference<>();
    private static AtomicReference<EventStreamReader<TxTimeoutRequest>> txnReader = new AtomicReference<>();
    private static AtomicReference<EventStreamWriter<TxTimeoutRequest>> txnWriter = new AtomicReference<>();

    private static AtomicReference<ScaleRequestHandler> scaleHandler = new AtomicReference<>();
    private static AtomicReference<EventStreamReader<ScaleRequest>> scaleReader = new AtomicReference<>();
    private static AtomicReference<EventStreamWriter<ScaleRequest>> scaleWriter = new AtomicReference<>();
    private static AtomicReference<RequestReader<ScaleRequest, ScaleRequestHandler>> scaleRequestReader = new AtomicReference<>();

    public static void bootstrap(ControllerService controllerService, ScheduledExecutorService executor, ClientFactory clientFactory) {

        CompletableFuture<Void> createStream = new CompletableFuture<>();
        CompletableFuture<Void> createTxnReader = new CompletableFuture<>();
        CompletableFuture<Void> createScaleReader = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> createStreams(controllerService, executor, createStream));

        createStream.thenAccept(x -> startTxnReader(clientFactory,
                controllerService.getStreamStore(), controllerService.getStreamTransactionMetadataTasks(),
                executor, createTxnReader));

        createTxnReader.thenAccept(x -> startScaleReader(clientFactory,
                controllerService.getStreamMetadataTasks(), controllerService.getStreamStore(),
                controllerService.getStreamTransactionMetadataTasks(), executor, createScaleReader));
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

    private static void createStreams(ControllerService controllerService, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        retryIndefinitely(() -> {

            CompletableFuture<CreateStreamStatus> requestStreamFuture = streamCreationCompletionCallback(
                    controllerService.createStream(REQUEST_STREAM_CONFIG, System.currentTimeMillis()));
            CompletableFuture<CreateStreamStatus> txnStreamFuture = streamCreationCompletionCallback(
                    controllerService.createStream(TXN_TIMER_STREAM_CONFIG, System.currentTimeMillis()));

            FutureHelpers.getAndHandleExceptions(CompletableFuture.allOf(
                    requestStreamFuture,
                    txnStreamFuture),
                    RuntimeException::new);
            return null;
        }, executor, result);
    }

    private static void startTxnReader(ClientFactory clientFactory, StreamMetadataStore streamStore, StreamTransactionMetadataTasks streamTransactionMetadataTasks, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        retryIndefinitely(() -> {
            if (txnHandler.get() == null) {
                txnHandler.compareAndSet(null, new TransactionTimer(streamTransactionMetadataTasks));
            }

            // PositionImpl position = new PositionImpl(Collections.singletonMap(new Segment(Config.INTERNAL_SCOPE, Config.TXN_TIMER_STREAM_NAME, 0), 0L));

            if (txnReader.get() == null) {
                txnReader.compareAndSet(null, clientFactory.createReader(Config.TXN_READER_ID,
                        Config.TXN_READER_GROUP,
                        new JavaSerializer<>(),
                        new ReaderConfig()));
                // txnReader.compareAndSet(null, clientFactory.createReader(Config.TXN_TIMER_STREAM_NAME,
                //         new JavaSerializer<>(),
                //         new ReaderConfig(),
                //         position
                // ));
            }

            if (txnWriter.get() == null) {
                txnWriter.compareAndSet(null, clientFactory.createEventWriter(Config.TXN_TIMER_STREAM_NAME,
                        new JavaSerializer<>(),
                        new EventWriterConfig(null)));
            }

            if (txnRequestReader.get() == null) {
                txnRequestReader.compareAndSet(null, new RequestReader<>(
                        Config.TXN_READER_ID,
                        Config.TXN_READER_GROUP,
                        txnWriter.get(),
                        txnReader.get(),
                        streamStore,
                        txnHandler.get(),
                        executor));
            }
            CompletableFuture.runAsync(txnRequestReader.get(), Executors.newSingleThreadExecutor());
            return null;
        }, executor, result);
    }

    private static void startScaleReader(ClientFactory clientFactory, StreamMetadataTasks streamMetadataTasks, StreamMetadataStore streamStore, StreamTransactionMetadataTasks streamTransactionMetadataTasks, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        retryIndefinitely(() -> {
            if (scaleHandler.get() == null) {
                scaleHandler.compareAndSet(null, new ScaleRequestHandler(streamMetadataTasks, streamStore, streamTransactionMetadataTasks));
            }

            PositionImpl position = new PositionImpl(Collections.singletonMap(new Segment(Config.INTERNAL_SCOPE, Config.SCALE_STREAM_NAME, 0), 0L));

            if (scaleReader.get() == null) {
                scaleReader.compareAndSet(null, clientFactory.createReader(Config.SCALE_READER_ID,
                        Config.SCALE_READER_GROUP,
                        new JavaSerializer<>(),
                        new ReaderConfig()));
                // scaleReader.compareAndSet(null, clientFactory.createReader(Config.SCALE_STREAM_NAME,
                //         new JavaSerializer<>(),
                //         new ReaderConfig(),
                //         position));
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
