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
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.lang.ProcessStarter;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.segmentstore.storage.impl.bookkeeper.ZooKeeperServiceRunner;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import io.pravega.test.integration.selftest.TestLogger;
import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
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

    private static final String LOG_ID = "SegmentStoreAdapter";
    private final ScheduledExecutorService testExecutor;
    private final TestConfig config;
    private final AtomicBoolean closed;
    private final AtomicBoolean initialized;
    private final ServiceBuilder serviceBuilder;
    private final AtomicReference<Storage> storage;
    private final AtomicReference<ScheduledExecutorService> storeExecutor;
    private final Thread stopBookKeeperProcess;
    private Process bookKeeperService;
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
    SegmentStoreAdapter(TestConfig testConfig, ServiceBuilderConfig builderConfig, ScheduledExecutorService testExecutor) {
        this.config = Preconditions.checkNotNull(testConfig, "testConfig");
        Preconditions.checkNotNull(builderConfig, "builderConfig");
        this.closed = new AtomicBoolean();
        this.initialized = new AtomicBoolean();
        this.storage = new AtomicReference<>();
        this.storeExecutor = new AtomicReference<>();
        this.testExecutor = Preconditions.checkNotNull(testExecutor, "testExecutor");
        this.serviceBuilder = attachDataLogFactory(ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(setup.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> {
                    // We use the Segment Store Executor for the real storage.
                    SingletonStorageFactory factory = new SingletonStorageFactory(setup.getExecutor());
                    this.storage.set(factory.createStorageAdapter());

                    // A bit hack-ish, but we need to get a hold of the Store Executor, so we can request snapshots for it.
                    this.storeExecutor.set(setup.getExecutor());
                    return factory;
                }));
        this.stopBookKeeperProcess = new Thread(this::stopBookKeeper);
        Runtime.getRuntime().addShutdownHook(this.stopBookKeeperProcess);
    }

    private ServiceBuilder attachDataLogFactory(ServiceBuilder builder) {
        if (this.config.getBookieCount() > 0) {
            // We were instructed to start at least one Bookie.
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
            // No Bookies -> InMemory Tier1.
            return builder.withDataLogFactory(setup -> new InMemoryDurableDataLogFactory(setup.getExecutor()));
        }
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            this.serviceBuilder.close();
            stopBookKeeper();

            val zk = this.zkClient;
            if (zk != null) {
                zk.close();
                this.zkClient = null;
            }

            this.closed.set(true);
            Runtime.getRuntime().removeShutdownHook(this.stopBookKeeperProcess);
            TestLogger.log(LOG_ID, "Closed.");
        }
    }

    private void stopBookKeeper() {
        val bk = this.bookKeeperService;
        if (bk != null) {
            bk.destroyForcibly();
            TestLogger.log(LOG_ID, "Bookies shut down.");
            this.bookKeeperService = null;
        }
    }

    //endregion

    //region StoreAdapter Implementation

    @Override
    public void initialize() throws Exception {
        Preconditions.checkState(!this.initialized.get(), "Cannot call initialize() after initialization happened.");
        TestLogger.log(LOG_ID, "Initializing.");
        if (this.config.getBookieCount() > 0) {
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
    public StoreReader createReader() {
        return new SegmentStoreReader(this.config, this.streamSegmentStore, this.storage.get(), this.testExecutor);
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
    public CompletableFuture<Void> abortTransaction(String transactionName, Duration timeout) {
        // At the SegmentStore level, aborting transactions means deleting their segments.
        ensureInitializedAndNotClosed();
        return this.delete(transactionName, timeout);
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
                SegmentProperties sp = this.streamSegmentStore.getStreamSegmentInfo(segmentName, false, timeout)
                                                              .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
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

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.initialized.get(), "initialize() must be called before invoking this operation.");
    }

    StreamSegmentStore getStreamSegmentStore() {
        return this.streamSegmentStore;
    }

    private Process startBookKeeper() throws Exception {
        int bookieCount = this.config.getBookieCount();
        Process p = ProcessStarter
                .forClass(BookKeeperServiceRunner.class)
                .sysProp(BookKeeperServiceRunner.PROPERTY_BASE_PORT, this.config.getBkPort(0))
                .sysProp(BookKeeperServiceRunner.PROPERTY_BOOKIE_COUNT, bookieCount)
                .sysProp(BookKeeperServiceRunner.PROPERTY_ZK_PORT, this.config.getZkPort())
                .sysProp(BookKeeperServiceRunner.PROPERTY_LEDGERS_PATH, TestConfig.BK_LEDGER_PATH)
                .sysProp(BookKeeperServiceRunner.PROPERTY_START_ZK, true)
                .stdOut(ProcessBuilder.Redirect.to(new File(this.config.getComponentOutLogPath("bk", 0))))
                .stdErr(ProcessBuilder.Redirect.to(new File(this.config.getComponentErrLogPath("bk", 0))))
                .start();
        ZooKeeperServiceRunner.waitForServerUp(this.config.getZkPort());
        TestLogger.log(LOG_ID, "Zookeeper (Port %s) and BookKeeper (Ports %s-%s) started.",
                this.config.getZkPort(), this.config.getBkPort(0), this.config.getBkPort(bookieCount - 1));
        return p;
    }

    //endregion

    //region SingletonStorageFactory

    private static class SingletonStorageFactory implements StorageFactory, AutoCloseable {
        private final AtomicBoolean closed;
        private final Storage storage;

        SingletonStorageFactory(ScheduledExecutorService executor) {
            this.storage = new InMemoryStorageFactory(executor).createStorageAdapter();
            this.storage.initialize(1);
            this.closed = new AtomicBoolean();
        }

        @Override
        public Storage createStorageAdapter() {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.storage;
        }

        @Override
        public void close() {
            if (!this.closed.get()) {
                this.storage.close();
                this.closed.set(true);
            }
        }
    }

    //endregion

}
