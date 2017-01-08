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

package com.emc.pravega.service.server.host.selftest;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.concurrent.ExecutorServiceHelpers;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.storage.TruncateableStorage;
import com.emc.pravega.service.storage.impl.rocksdb.RocksDBCacheFactory;
import com.emc.pravega.service.storage.impl.rocksdb.RocksDBConfig;
import com.emc.pravega.service.storage.mocks.InMemoryDurableDataLogFactory;
import com.emc.pravega.service.storage.mocks.InMemoryStorageFactory;
import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Store Adapter wrapping a real StreamSegmentStore.
 */
class StreamSegmentStoreAdapter implements StoreAdapter {
    //region Members

    private static final String LOG_ID = "SegmentStoreAdapter";
    private static final Consumer<Long> LONG_TO_VOID = ignored -> {
    };
    private final AtomicBoolean closed;
    private final AtomicBoolean initialized;
    private final ServiceBuilder serviceBuilder;
    private final AtomicReference<VerificationStorage> storage;
    private final AtomicReference<ExecutorService> storeExecutor; // Only for reporting; do not use.
    private StreamSegmentStore streamSegmentStore;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentStoreAdapter class.
     *
     * @param builderConfig        The ServiceBuilderConfig to use.
     * @param testCallbackExecutor An Executor to use for test-related async operations.
     */
    StreamSegmentStoreAdapter(TestConfig testConfig, ServiceBuilderConfig builderConfig, Executor testCallbackExecutor) {
        Preconditions.checkNotNull(testConfig, "testConfig");
        Preconditions.checkNotNull(builderConfig, "builderConfig");
        Preconditions.checkNotNull(testCallbackExecutor, "testCallbackExecutor");
        this.closed = new AtomicBoolean();
        this.initialized = new AtomicBoolean();
        this.storage = new AtomicReference<>();
        this.storeExecutor = new AtomicReference<>();
        this.serviceBuilder = ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withDataLogFactory(setup -> {
                    InMemoryDurableDataLogFactory factory = new InMemoryDurableDataLogFactory(setup.getExecutor());
                    Duration appendDelay = testConfig.getDataLogAppendDelay();
                    factory.setAppendDelayProvider(() -> appendDelay);
                    return factory;
                })
                .withCacheFactory(setup -> new RocksDBCacheFactory(setup.getConfig(RocksDBConfig::new)))
                .withStorageFactory(setup -> {
                    // We use the Segment Store Executor for the real storage.
                    TruncateableStorage innerStorage = new InMemoryStorageFactory(setup.getExecutor()).getStorageAdapter();

                    // ... and the Test executor for the verification storage (to invoke callbacks).
                    VerificationStorage.Factory factory = new VerificationStorage.Factory(innerStorage, testCallbackExecutor);
                    this.storage.set((VerificationStorage) factory.getStorageAdapter());

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
    public CompletableFuture<Void> initialize(Duration timeout) {
        Preconditions.checkState(!this.initialized.get(), "Cannot call initialize() after initialization happened.");
        TestLogger.log(LOG_ID, "Initializing.");
        return this.serviceBuilder.initialize()
                                  .thenRun(() -> {
                                      this.streamSegmentStore = this.serviceBuilder.createStreamSegmentService();
                                      this.initialized.set(true);
                                      TestLogger.log(LOG_ID, "Up and running.");
                                  });
    }

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, byte[] data, AppendContext context, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.append(streamSegmentName, data, context, timeout);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.getStreamSegmentInfo(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.read(streamSegmentName, offset, maxLength, timeout);
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.createStreamSegment(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStreamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.createTransaction(parentStreamSegmentName, UUID.randomUUID(), timeout);
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.mergeTransaction(transactionName, timeout).thenAccept(LONG_TO_VOID);
    }

    @Override
    public CompletableFuture<Void> sealStreamSegment(String streamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return this.streamSegmentStore.sealStreamSegment(streamSegmentName, timeout).thenAccept(LONG_TO_VOID);
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

    //endregion

    //region Helpers

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.initialized.get(), "initialize() must be called before invoking this operation.");
    }

    //endregion
}
