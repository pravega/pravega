/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.service.selftest;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.service.contracts.AttributeUpdate;
import io.pravega.service.contracts.ReadResult;
import io.pravega.service.contracts.SegmentProperties;
import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.server.store.ServiceBuilder;
import io.pravega.service.server.store.ServiceBuilderConfig;
import io.pravega.service.storage.TruncateableStorage;
import io.pravega.service.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.service.storage.impl.rocksdb.RocksDBConfig;
import io.pravega.service.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.service.storage.mocks.InMemoryStorageFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Store Adapter wrapping a real StreamSegmentStore.
 */
class StreamSegmentStoreAdapter implements StoreAdapter {
    //region Members

    private static final String LOG_ID = "SegmentStoreAdapter";
    protected final Executor testExecutor;
    private final AtomicBoolean closed;
    private final AtomicBoolean initialized;
    private final ServiceBuilder serviceBuilder;
    private final AtomicReference<VerificationStorage> storage;
    private final AtomicReference<ExecutorService> storeExecutor;
    private StreamSegmentStore streamSegmentStore;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentStoreAdapter class.
     *
     * @param builderConfig The Test Configuration to use.
     * @param builderConfig The ServiceBuilderConfig to use.
     * @param testExecutor  An Executor to use for test-related async operations.
     */
    StreamSegmentStoreAdapter(TestConfig testConfig, ServiceBuilderConfig builderConfig, Executor testExecutor) {
        Preconditions.checkNotNull(testConfig, "testConfig");
        Preconditions.checkNotNull(builderConfig, "builderConfig");
        Preconditions.checkNotNull(testExecutor, "testCallbackExecutor");
        this.closed = new AtomicBoolean();
        this.initialized = new AtomicBoolean();
        this.storage = new AtomicReference<>();
        this.storeExecutor = new AtomicReference<>();
        this.testExecutor = testExecutor;
        this.serviceBuilder = ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withDataLogFactory(setup -> {
                    InMemoryDurableDataLogFactory factory = new InMemoryDurableDataLogFactory(setup.getExecutor());
                    Duration appendDelay = testConfig.getDataLogAppendDelay();
                    factory.setAppendDelayProvider(() -> appendDelay);
                    return factory;
                })
                .withCacheFactory(setup -> new RocksDBCacheFactory(setup.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> {
                    // We use the Segment Store Executor for the real storage.
                    TruncateableStorage innerStorage = new InMemoryStorageFactory(setup.getExecutor()).createStorageAdapter();
                    innerStorage.initialize(0);

                    // ... and the Test executor for the verification storage (to invoke callbacks).
                    VerificationStorage.Factory factory = new VerificationStorage.Factory(innerStorage, testExecutor);
                    this.storage.set((VerificationStorage) factory.createStorageAdapter());

                    // A bit hack-ish, but we need to get a hold of the Store Executor, so we can request snapshots for it.
                    this.storeExecutor.set(setup.getExecutor());
                    return factory;
                });
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            this.serviceBuilder.close();
            this.closed.set(true);
            TestLogger.log(LOG_ID, "Closed.");
        }
    }

    //endregion

    //region StoreAdapter Implementation

    @Override
    public void initialize() throws Exception {
        Preconditions.checkState(!this.initialized.get(), "Cannot call initialize() after initialization happened.");
        TestLogger.log(LOG_ID, "Initializing.");
        this.serviceBuilder.initialize();
        this.streamSegmentStore = this.serviceBuilder.createStreamSegmentService();
        this.initialized.set(true);
        TestLogger.log(LOG_ID, "Up and running.");
    }

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.append(streamSegmentName, data, attributeUpdates, timeout);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.getStreamSegmentInfo(streamSegmentName, false, timeout);
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.read(streamSegmentName, offset, maxLength, timeout);
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.createStreamSegment(streamSegmentName, attributes, timeout);
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStreamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.createTransaction(parentStreamSegmentName, UUID.randomUUID(), attributes, timeout);
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.mergeTransaction(transactionName, timeout);
    }

    @Override
    public CompletableFuture<Void> sealStreamSegment(String streamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return FutureHelpers.toVoid(this.streamSegmentStore.sealStreamSegment(streamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.deleteStreamSegment(streamSegmentName, timeout);
    }

    @Override
    public VerificationStorage getStorageAdapter() {
        return this.storage.get();
    }

    public ExecutorServiceHelpers.Snapshot getStorePoolSnapshot() {
        return this.storeExecutor.get() != null ? ExecutorServiceHelpers.getSnapshot(this.storeExecutor.get()) : null;
    }

    @Override
    public boolean isFeatureSupported(Feature feature) {
        return true; // All features are supported
    }

    //endregion

    //region Helpers

    protected void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.initialized.get(), "initialize() must be called before invoking this operation.");
    }

    protected StreamSegmentStore getStreamSegmentStore() {
        return this.streamSegmentStore;
    }

    //endregion
}
