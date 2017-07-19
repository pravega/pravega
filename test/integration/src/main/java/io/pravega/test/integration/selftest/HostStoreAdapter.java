/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.val;

/**
 * Store adapter wrapping a real StreamSegmentStore and Connection Listener.
 */
public class HostStoreAdapter extends SegmentStoreAdapter {
    private static final String LOG_ID = "HostStoreAdapter";
    private static final String SCOPE = "scope";
    private static final String LISTENING_ADDRESS = "localhost";
    private final int listeningPort;
    private final boolean autoFlush;
    private final int writerCount;
    private final ConcurrentHashMap<String, WriterCollection> writers;
    private PravegaConnectionListener listener;
    private MockStreamManager streamManager;

    /**
     * Creates a new instance of the HostStoreAdapter class.
     *
     * @param testConfig    The TestConfig to use.
     * @param builderConfig The ServiceBuilderConfig to use.
     * @param testExecutor  An Executor to use for test-related async operations.
     */
    HostStoreAdapter(TestConfig testConfig, ServiceBuilderConfig builderConfig, ScheduledExecutorService testExecutor) {
        super(testConfig, builderConfig, testExecutor);
        this.listeningPort = testConfig.getClientPort();
        this.autoFlush = testConfig.isClientAutoFlush();
        this.writerCount = testConfig.getClientWriterCount();
        this.writers = new ConcurrentHashMap<>();
    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.writers.values().forEach(WriterCollection::close);
        this.writers.clear();

        if (this.streamManager != null) {
            this.streamManager.close();
            this.streamManager = null;
        }

        if (this.listener != null) {
            this.listener.close();
            this.listener = null;
        }

        TestLogger.log(LOG_ID, "Closed.");
        super.close();
    }

    //endregion

    //region StoreAdapter Implementation

    @Override
    public boolean isFeatureSupported(Feature feature) {
        return feature == Feature.Create
                || feature == Feature.Append;
    }

    @Override
    public void initialize() throws Exception {
        super.initialize();
        this.listener = new PravegaConnectionListener(false, this.listeningPort, getStreamSegmentStore());
        this.listener.startListening();
        this.streamManager = new MockStreamManager(SCOPE, LISTENING_ADDRESS, this.listeningPort);
        this.streamManager.createScope(SCOPE);
        TestLogger.log(LOG_ID, "Initialized.");
    }

    @Override
    public CompletableFuture<Void> createStream(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.runAsync(() -> {
            if (this.writers.containsKey(streamName)) {
                throw new CompletionException(new StreamSegmentExistsException(streamName));
            }

            this.streamManager.createStream(SCOPE, streamName, null);
            WriterCollection producers = new WriterCollection(streamName, this.writerCount, this.streamManager);
            this.writers.putIfAbsent(streamName, producers);
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> delete(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("delete is not supported.");
    }

    @Override
    public CompletableFuture<Void> append(String streamName, Event event, Duration timeout) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.runAsync(() -> {
            WriterCollection segmentWriterCollection = this.writers.getOrDefault(streamName, null);
            if (segmentWriterCollection == null) {
                throw new CompletionException(new StreamSegmentNotExistsException(streamName));
            }

            EventStreamWriter<byte[]> writer = segmentWriterCollection.next();
            ArrayView s = event.getSerialization();
            byte[] payload = s.arrayOffset() == 0 ? s.array() : Arrays.copyOfRange(s.array(), s.arrayOffset(), s.getLength());
            Future<Void> r = writer.writeEvent(Integer.toString(event.getRoutingKey()), payload);
            if (this.autoFlush) {
                writer.flush();
            }

            try {
                r.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> seal(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("seal is not supported.");
    }

    @Override
    public CompletableFuture<SegmentProperties> getInfo(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("getInfo is not supported.");
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStream, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("transactions are not supported.");
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("transactions are not supported.");
    }

    //endregion

    private static class WriterCollection implements AutoCloseable {
        private static final ByteArraySerializer SERIALIZER = new ByteArraySerializer();
        private static final EventWriterConfig WRITER_CONFIG = EventWriterConfig.builder().build();
        private final List<EventStreamWriter<byte[]>> writers;
        private final AtomicInteger nextWriterId;

        WriterCollection(String segmentName, int count, MockStreamManager streamManager) {
            this.writers = Collections.synchronizedList(new ArrayList<>(count));
            this.nextWriterId = new AtomicInteger();
            for (int i = 0; i < count; i++) {
                val writer = streamManager.getClientFactory()
                                          .createEventWriter(segmentName,
                                                  SERIALIZER,
                                                  WRITER_CONFIG);
                this.writers.add(writer);
            }
        }

        EventStreamWriter<byte[]> next() {
            return this.writers.get(this.nextWriterId.getAndIncrement() % this.writers.size());
        }

        @Override
        public void close() {
            this.writers.forEach(EventStreamWriter::close);
        }
    }
}
