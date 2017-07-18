/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Store Adapter wrapping a StreamSegmentStore directly. Every "Stream" is actually a single Segment. Routing keys are
 * ignored.
 */
class SegmentStoreAdapter implements StoreAdapter {
    //region Members

    static final String BK_LEDGER_PATH = "/pravega/selftest/bookkeeper/ledgers";
    private static final String LOG_ID = "SegmentStoreAdapter";
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
     * Creates a new instance of the SegmentStoreAdapter class.
     *
     * @param testConfig    The Test Configuration to use.
     * @param builderConfig The ServiceBuilderConfig to use.
     * @param testExecutor  An Executor to use for test-related async operations.
     */
    SegmentStoreAdapter(TestConfig testConfig, ServiceBuilderConfig builderConfig, Executor testExecutor) {
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
                BookKeeperConfig bkConfig = setup.getConfig(BookKeeperConfig::builder);
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
    public CompletableFuture<Void> append(String streamName, Event event, Duration timeout) {
        ensureInitializedAndNotClosed();
        ArrayView s = event.getSerialization();
        byte[] payload = s.arrayOffset() == 0 ? s.array() : Arrays.copyOfRange(s.array(), s.arrayOffset(), s.getLength());
        return this.streamSegmentStore.append(streamName, payload, null, timeout)
                                      .exceptionally(ex -> attemptReconcile(ex, streamName, timeout));
    }

    @Override
    public CompletableFuture<SegmentProperties> getInfo(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.getStreamSegmentInfo(streamName, false, timeout);
    }

    @Override
    public StoreReader createReader() {
        return new SegmentStoreReader(this.streamSegmentStore, this.storage.get(), this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> createStream(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.createStreamSegment(streamName, null, timeout);
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStream, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.createTransaction(parentStream, UUID.randomUUID(), null, timeout);
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        ensureInitializedAndNotClosed();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.streamSegmentStore
                .sealStreamSegment(transactionName, timer.getRemaining())
                .thenCompose(v -> this.streamSegmentStore.mergeTransaction(transactionName, timer.getRemaining()));
    }

    @Override
    public CompletableFuture<Void> seal(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return FutureHelpers.toVoid(this.streamSegmentStore.sealStreamSegment(streamName, timeout));
    }

    @Override
    public CompletableFuture<Void> delete(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.deleteStreamSegment(streamName, timeout);
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

    @SneakyThrows
    private Void attemptReconcile(Throwable ex, String segmentName, Duration timeout) {
        ex = ExceptionHelpers.getRealException(ex);
        boolean reconciled = false;
        if (isPossibleEndOfSegment(ex)) {
            // If we get a Sealed/Merged/NotExists exception, verify that the segment really is in that state.
            try {
                SegmentProperties sp = getInfo(segmentName, timeout).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                reconciled = sp.isSealed() || sp.isDeleted();
            } catch (Throwable ex2) {
                reconciled = isPossibleEndOfSegment(ExceptionHelpers.getRealException(ex2));
            }
        }

        if (reconciled) {
            return null;
        } else {
            throw ex;
        }
    }

    private boolean isPossibleEndOfSegment(Throwable ex) {
        return ex instanceof StreamSegmentSealedException
                || ex instanceof StreamSegmentNotExistsException
                || ex instanceof StreamSegmentMergedException;
    }

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
        runner.startAll();
        return runner;
    }

    //endregion
}
