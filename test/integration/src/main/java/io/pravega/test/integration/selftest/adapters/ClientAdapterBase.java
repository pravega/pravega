/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest.adapters;

import com.google.common.base.Preconditions;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import io.pravega.test.integration.selftest.TestLogger;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;

/**
 * Store adapter wrapping a real Pravega Client.
 */
abstract class ClientAdapterBase implements StoreAdapter {
    //region Members
    private static final ByteArraySerializer SERIALIZER = new ByteArraySerializer();
    private static final EventWriterConfig WRITER_CONFIG = EventWriterConfig.builder().build();
    private static final String LOG_ID = "ClientAdapter";
    private static final String SCOPE = "scope";
    private static final String LISTENING_ADDRESS = "localhost";
    protected final int listeningPort;
    private final ScheduledExecutorService testExecutor;
    private final ConcurrentHashMap<String, EventStreamWriter<byte[]>> writers;
    private final AtomicBoolean closed;
    private final AtomicBoolean initialized;
    private MockStreamManager streamManager;

    //endregion

    /**
     * Creates a new instance of the ClientAdapterBase class.
     *
     * @param testConfig    The TestConfig to use.
     */
    ClientAdapterBase(TestConfig testConfig, ScheduledExecutorService testExecutor) {
        this.listeningPort = testConfig.getClientPort();
        this.testExecutor = Preconditions.checkNotNull(testExecutor, "testExecutor");
        this.writers = new ConcurrentHashMap<>();
        this.closed = new AtomicBoolean();
        this.initialized = new AtomicBoolean();

    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.writers.values().forEach(EventStreamWriter::close);
            this.writers.clear();

            if (this.streamManager != null) {
                this.streamManager.close();
                this.streamManager = null;
            }

            TestLogger.log(LOG_ID, "Closed.");
        }
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
        Preconditions.checkState(!this.initialized.getAndSet(true), "Cannot call initialize() after initialization happened.");
        TestLogger.log(LOG_ID, "Initializing.");
        this.streamManager = new MockStreamManager(SCOPE, LISTENING_ADDRESS, this.listeningPort);
        this.streamManager.createScope(SCOPE);
        this.initialized.set(true);
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
            EventStreamWriter<byte[]> writer = this.streamManager.getClientFactory()
                    .createEventWriter(streamName,
                            SERIALIZER,
                            WRITER_CONFIG);
            this.writers.putIfAbsent(streamName, writer);
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> delete(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("delete is not supported.");
    }

    @Override
    @SneakyThrows(StreamSegmentNotExistsException.class)
    public CompletableFuture<Void> append(String streamName, Event event, Duration timeout) {
        ensureInitializedAndNotClosed();
        EventStreamWriter<byte[]> writer = this.writers.getOrDefault(streamName, null);
        if (writer == null) {
            throw new StreamSegmentNotExistsException(streamName);
        }

        ArrayView s = event.getSerialization();
        byte[] payload = s.arrayOffset() == 0 ? s.array() : Arrays.copyOfRange(s.array(), s.arrayOffset(), s.getLength());
        return writer.writeEvent(Integer.toString(event.getRoutingKey()), payload);
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

    @Override
    public StoreReader createReader() {
        return null;
    }

    @Override
    public ExecutorServiceHelpers.Snapshot getStorePoolSnapshot() {
        return null;
    }

    //endregion

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.initialized.get(), "initialize() must be called before invoking this operation.");
    }
}
