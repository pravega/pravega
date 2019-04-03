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
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Store Adapter wrapping a StreamSegmentStore directly. Every "Stream" is actually a single Segment. Routing keys are
 * ignored.
 */
class SegmentStoreAdapter extends StoreAdapter {
    //region Members

    private final ScheduledExecutorService testExecutor;
    private final TestConfig config;
    private final ServiceBuilderConfig builderConfig;
    private final ServiceBuilder serviceBuilder;
    private final AtomicReference<Storage> storage;
    private final AtomicReference<ScheduledExecutorService> storeExecutor;
    private final Thread stopBookKeeperProcess;
    private Process bookKeeperService;
    private StreamSegmentStore streamSegmentStore;
    private TableStore tableStore;
    private CuratorFramework zkClient;
    private StatsProvider statsProvider;


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
        this.builderConfig = Preconditions.checkNotNull(builderConfig, "builderConfig");
        this.storage = new AtomicReference<>();
        this.storeExecutor = new AtomicReference<>();
        this.testExecutor = Preconditions.checkNotNull(testExecutor, "testExecutor");
        this.serviceBuilder = attachDataLogFactory(ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(setup.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> {
                    // We use the Segment Store Executor for the real storage.
                    SingletonStorageFactory factory = new SingletonStorageFactory(setup.getStorageExecutor());
                    this.storage.set(factory.createStorageAdapter());

                    // A bit hack-ish, but we need to get a hold of the Store Executor, so we can request snapshots for it.
                    this.storeExecutor.set(setup.getCoreExecutor());
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
                return new BookKeeperLogFactory(bkConfig, this.zkClient, setup.getCoreExecutor());
            });
        } else {
            // No Bookies -> InMemory Tier1.
            return builder.withDataLogFactory(setup -> new InMemoryDurableDataLogFactory(setup.getCoreExecutor()));
        }
    }

    //endregion

    //region AbstractIdleService Implementation

    @Override
    protected void startUp() throws Exception {
        if (this.config.isMetricsEnabled()) {
            MetricsProvider.initialize(this.builderConfig.getConfig(MetricsConfig::builder));
            this.statsProvider = MetricsProvider.getMetricsProvider();
            this.statsProvider.start();
        }

        if (this.config.getBookieCount() > 0) {
            this.bookKeeperService = BookKeeperAdapter.startBookKeeperOutOfProcess(this.config, this.logId);
        }

        this.serviceBuilder.initialize();
        this.streamSegmentStore = this.serviceBuilder.createStreamSegmentService();
        this.tableStore = this.serviceBuilder.createTableStoreService();
    }

    @Override
    protected void shutDown() {
        this.serviceBuilder.close();
        stopBookKeeper();

        val zk = this.zkClient;
        if (zk != null) {
            zk.close();
            this.zkClient = null;
        }

        StatsProvider sp = this.statsProvider;
        if (sp != null) {
            sp.close();
            this.statsProvider = null;
        }

        Runtime.getRuntime().removeShutdownHook(this.stopBookKeeperProcess);
    }

    //endregion

    //region Stream Operations

    @Override
    public CompletableFuture<Void> append(String streamName, Event event, Duration timeout) {
        ensureRunning();
        ArrayView s = event.getSerialization();
        byte[] payload = s.arrayOffset() == 0 ? s.array() : Arrays.copyOfRange(s.array(), s.arrayOffset(), s.getLength());
        return this.streamSegmentStore.append(streamName, payload, null, timeout)
                                      .exceptionally(ex -> attemptReconcile(ex, streamName, timeout));
    }

    @Override
    public StoreReader createReader() {
        ensureRunning();
        return new SegmentStoreReader(this.config, this.streamSegmentStore, this.storage.get(), this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> createStream(String streamName, Duration timeout) {
        ensureRunning();
        return this.streamSegmentStore.createStreamSegment(streamName, null, timeout);
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStream, Duration timeout) {
        ensureRunning();

        // Generate a transaction name. This need not be the same as what the Client would do, but we need a unique
        // name for the new segment. In mergeTransaction, we need a way to extract the original Segment's name out of this
        // txnName, so best if we use the StreamSegmentNameUtils class.
        String txnName = StreamSegmentNameUtils.getTransactionNameFromId(parentStream, UUID.randomUUID());
        return this.streamSegmentStore.createStreamSegment(txnName, null, timeout)
                                      .thenApply(v -> txnName);
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        ensureRunning();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        String parentSegment = StreamSegmentNameUtils.getParentStreamSegmentName(transactionName);
        return Futures.toVoid(this.streamSegmentStore.mergeStreamSegment(parentSegment, transactionName, timer.getRemaining()));
    }

    @Override
    public CompletableFuture<Void> abortTransaction(String transactionName, Duration timeout) {
        // At the SegmentStore level, aborting transactions means deleting their segments.
        ensureRunning();
        return this.deleteStream(transactionName, timeout);
    }

    @Override
    public CompletableFuture<Void> sealStream(String streamName, Duration timeout) {
        ensureRunning();
        return Futures.toVoid(this.streamSegmentStore.sealStreamSegment(streamName, timeout));
    }

    @Override
    public CompletableFuture<Void> deleteStream(String streamName, Duration timeout) {
        ensureRunning();
        return this.streamSegmentStore.deleteStreamSegment(streamName, timeout);
    }

    //endregion

    //region Table Operations

    @Override
    public CompletableFuture<Void> createTable(String tableName, Duration timeout) {
        ensureRunning();
        return this.tableStore.createSegment(tableName, timeout);
    }

    @Override
    public CompletableFuture<Void> deleteTable(String tableName, Duration timeout) {
        ensureRunning();
        return this.tableStore.deleteSegment(tableName, false, timeout);
    }

    @Override
    public CompletableFuture<Long> updateTableEntry(String tableName, ArrayView key, ArrayView value, Long compareVersion, Duration timeout) {
        ensureRunning();
        TableEntry e = compareVersion == null || compareVersion == TableKey.NO_VERSION
                ? TableEntry.unversioned(key, value)
                : TableEntry.versioned(key, value, compareVersion);
        return this.tableStore.put(tableName, Collections.singletonList(e), timeout)
                              .thenApply(versions -> versions.get(0));
    }

    @Override
    public CompletableFuture<Void> removeTableEntry(String tableName, ArrayView key, Long compareVersion, Duration timeout) {
        ensureRunning();
        TableKey e = compareVersion == null || compareVersion == TableKey.NO_VERSION
                ? TableKey.unversioned(key)
                : TableKey.versioned(key, compareVersion);
        return this.tableStore.remove(tableName, Collections.singletonList(e), timeout);
    }

    @Override
    public CompletableFuture<List<ArrayView>> getTableEntries(String tableName, List<ArrayView> keys, Duration timeout) {
        ensureRunning();
        return this.tableStore
                .get(tableName, keys, timeout)
                .thenApplyAsync(storeResult -> storeResult.stream().map(e -> e == null ? null : e.getValue()).collect(Collectors.toList()), this.testExecutor);
    }

    @Override
    public CompletableFuture<AsyncIterator<List<Map.Entry<ArrayView, ArrayView>>>> iterateTableEntries(String tableName, Duration timeout) {
        ensureRunning();
        return this.tableStore
                .entryIterator(tableName, null, timeout)
                .thenApply(iterator -> () ->
                        iterator.getNext().thenApply(item -> {
                            if (item == null) {
                                return null;
                            } else {
                                return item.getEntries().stream()
                                           .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey().getKey(), e.getValue()))
                                           .collect(Collectors.<Map.Entry<ArrayView, ArrayView>>toList());
                            }
                        }));
    }

    //endregion

    //region StoreAdapter implementation

    @Override
    public ExecutorServiceHelpers.Snapshot getStorePoolSnapshot() {
        return this.storeExecutor.get() != null ? ExecutorServiceHelpers.getSnapshot(this.storeExecutor.get()) : null;
    }

    @Override
    public boolean isFeatureSupported(Feature feature) {
        return true; // All features are supported
    }

    //endregion

    //region Helpers

    private void stopBookKeeper() {
        val bk = this.bookKeeperService;
        if (bk != null) {
            bk.destroyForcibly();
            log("Bookies shut down.");
            this.bookKeeperService = null;
        }
    }

    @SneakyThrows
    private Void attemptReconcile(Throwable ex, String segmentName, Duration timeout) {
        ex = Exceptions.unwrap(ex);
        boolean reconciled = false;
        if (isPossibleEndOfSegment(ex)) {
            // If we get a Sealed/Merged/NotExists exception, verify that the segment really is in that state.
            try {
                SegmentProperties sp = this.streamSegmentStore.getStreamSegmentInfo(segmentName, timeout)
                                                              .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                reconciled = sp.isSealed() || sp.isDeleted();
            } catch (Throwable ex2) {
                reconciled = isPossibleEndOfSegment(Exceptions.unwrap(ex2));
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

    StreamSegmentStore getStreamSegmentStore() {
        return this.streamSegmentStore;
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
