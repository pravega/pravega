/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.segmentstore.selftest;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.TruncateableStorage;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Store Adapter wrapping a real StreamSegmentStore.
 */
class StreamSegmentStoreAdapter implements StoreAdapter {
    //region Members

    private static final String LOG_ID = "SegmentStoreAdapter";
    private static final String BK_LEDGER_PATH = "/pravega/selftest/bookkeeper/ledgers";
    protected final Executor testExecutor;
    private final TestConfig config;
    private final AtomicBoolean closed;
    private final AtomicBoolean initialized;
    private final ServiceBuilder serviceBuilder;
    private final AtomicReference<VerificationStorage> storage;
    private final AtomicReference<ExecutorService> storeExecutor;
    private BookKeeperServiceRunner bookKeeperService;
    private StreamSegmentStore streamSegmentStore;
    private CuratorFramework zkClient;

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
        this.config = Preconditions.checkNotNull(testConfig, "testConfig");
        Preconditions.checkNotNull(builderConfig, "builderConfig");
        this.closed = new AtomicBoolean();
        this.initialized = new AtomicBoolean();
        this.storage = new AtomicReference<>();
        this.storeExecutor = new AtomicReference<>();
        this.testExecutor = Preconditions.checkNotNull(testExecutor, "testCallbackExecutor");
        this.serviceBuilder = attachDataLogFactory(ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(setup.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> {
                    // We use the Segment Store Executor for the real storage.
                    TruncateableStorage innerStorage = new InMemoryStorageFactory(setup.getExecutor()).createStorageAdapter();
                    innerStorage.initialize(1);

                    // ... and the Test executor for the verification storage (to invoke callbacks).
                    VerificationStorage.Factory factory = new VerificationStorage.Factory(innerStorage, testExecutor);
                    this.storage.set((VerificationStorage) factory.createStorageAdapter());

                    // A bit hack-ish, but we need to get a hold of the Store Executor, so we can request snapshots for it.
                    this.storeExecutor.set(setup.getExecutor());
                    return factory;
                }));
    }

    private ServiceBuilder attachDataLogFactory(ServiceBuilder builder) {
        if (this.config.isUseBookKeeper()) {
            this.zkClient = CuratorFrameworkFactory
                    .builder()
                    .connectString("localhost:" + this.config.getZkPort())
                    .namespace("pravega")
                    .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                    .sessionTimeoutMs(5000)
                    .connectionTimeoutMs(5000)
                    .build();
            this.zkClient.start();
            return builder.withDataLogFactory(setup -> {
                BookKeeperConfig bkConfig = BookKeeperConfig
                        .builder()
                        .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + this.config.getZkPort())
                        .with(BookKeeperConfig.ZK_METADATA_PATH, "/selftest/segmentstore/containers")
                        .with(BookKeeperConfig.BK_LEDGER_PATH, BK_LEDGER_PATH)
                        .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                        .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                        .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                        .build();
                return new BookKeeperLogFactory(bkConfig, this.zkClient, setup.getExecutor());
            });
        } else {
            return builder.withDataLogFactory(setup -> {
                InMemoryDurableDataLogFactory factory = new InMemoryDurableDataLogFactory(setup.getExecutor());
                Duration appendDelay = this.config.getDataLogAppendDelay();
                factory.setAppendDelayProvider(() -> appendDelay);
                return factory;
            });
        }
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    @SneakyThrows
    public void close() {
        if (!this.closed.get()) {
            this.serviceBuilder.close();
            val bk = this.bookKeeperService;
            if (bk != null) {
                bk.close();
                this.bookKeeperService = null;
            }

            val zk = this.zkClient;
            if (zk != null) {
                zk.close();
                this.zkClient = null;
            }

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
        if (this.config.isUseBookKeeper()) {
            this.bookKeeperService = startBookKeeper();
        }

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

    private BookKeeperServiceRunner startBookKeeper() throws Exception {
        val runner = BookKeeperServiceRunner.builder()
                                            .bookiePorts(Collections.singletonList(this.config.getBkPort()))
                                            .zkPort(this.config.getZkPort())
                                            .startZk(true)
                                            .ledgersPath(BK_LEDGER_PATH)
                                            .build();
        runner.start();
        return runner;
    }

    //endregion
}
