/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest.adapters;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import io.pravega.test.integration.selftest.TestLogger;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public abstract class StoreAdapter extends AbstractIdleService implements AutoCloseable {
    protected final String logId = this.getClass().getSimpleName();

    //region Startup/Shutdown

    @Override
    public final void close() {
        if (state() != State.TERMINATED && state() != State.FAILED) {
            try {
                this.stopAsync().awaitTerminated();
                log("Closed.");
            } catch (Exception ex) {
                log("Closed with exception '%s'.", ex);
            }
        }
    }

    @Override
    protected abstract void startUp() throws Exception;

    @Override
    protected abstract void shutDown();

    //endregion

    //region Stream Operations

    public abstract CompletableFuture<Void> append(String streamName, Event event, Duration timeout);

    public abstract StoreReader createReader();
    public abstract CompletableFuture<Void> createStream(String streamName, Duration timeout);

    public abstract CompletableFuture<String> createTransaction(String parentStream, Duration timeout);

    public abstract CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout);
    public abstract CompletableFuture<Void> abortTransaction(String transactionName, Duration timeout);

    public abstract CompletableFuture<Void> sealStream(String streamName, Duration timeout);

    public abstract CompletableFuture<Void> deleteStream(String streamName, Duration timeout);

    //endregion

    //region Table Operations

    public abstract CompletableFuture<Void> createTable(String tableName, Duration timeout);

    public abstract CompletableFuture<Void> deleteTable(String tableName, Duration timeout);

    /**
     * Updates a Table Entry in a Table.
     *
     * @param tableName      The name of the Table to update the entry in.
     * @param key            The Key to update.
     * @param value          The Value to associate with the Key.
     * @param compareVersion (Optional) If provided, the update will be conditioned on this being the current Key version.
     * @param timeout        Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the latest version of the Key.
     */
    public abstract CompletableFuture<Long> updateTableEntry(String tableName, BufferView key, BufferView value, Long compareVersion, Duration timeout);

    /**
     * Removes a Table Entry from a Table.
     *
     * @param tableName      The name of the Table to remove the key from.
     * @param key            The Key to remove.
     * @param compareVersion (Optional) If provided, the update will be conditioned on this being the current Key version.
     * @param timeout        Timeout for the operation.
     * @return A CompletableFuture that will be completed when the key has been removed.
     */
    public abstract CompletableFuture<Void> removeTableEntry(String tableName, BufferView key, Long compareVersion, Duration timeout);

    /**
     * Retrieves the latest value of for multiple Table Entry from a Table.
     *
     * @param tableName The name of the Table to retrieve the Entry from.
     * @param keys      The Keys to retrieve.
     * @param timeout   Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the result.
     */
    public abstract CompletableFuture<List<BufferView>> getTableEntries(String tableName, List<BufferView> keys, Duration timeout);

    /**
     * Iterates through all the Entries in a Table.
     *
     * @param tableName The name of the Table to iterate over.
     * @param timeout   Timeout for the operation.
     * @return A CompletableFuture that will return an {@link AsyncIterator} to iterate through all entries in the table.
     */
    public abstract CompletableFuture<AsyncIterator<List<Map.Entry<BufferView, BufferView>>>> iterateTableEntries(String tableName, Duration timeout);

    //endregion

    public abstract ExecutorServiceHelpers.Snapshot getStorePoolSnapshot();

    public abstract boolean isFeatureSupported(Feature feature);

    protected void ensureRunning() {
        Preconditions.checkState(state() == State.RUNNING, "%s is not running.", logId);
    }

    protected void log(String messageFormat, Object... args) {
        TestLogger.log(this.logId, messageFormat, args);
    }

    //endregion

    //region Factory

    public static StoreAdapter create(TestConfig testConfig, ServiceBuilderConfig builderConfig, ScheduledExecutorService executor) {
        StoreAdapter result;
        switch (testConfig.getTestType()) {
            case SegmentStore:
            // case SegmentStoreTable:
            //     result = new SegmentStoreAdapter(testConfig, builderConfig, executor);
            //     break;
            // case AppendProcessor:
            //     result = new AppendProcessorAdapter(testConfig, builderConfig, executor);
            //     break;
            case InProcessMock:
                result = new InProcessMockClientAdapter(testConfig, executor);
                break;
            // case InProcessStore:
            //     result = new InProcessListenerWithRealStoreAdapter(testConfig, builderConfig, executor);
            //     break;
            // case OutOfProcess:
            //     result = new OutOfProcessAdapter(testConfig, builderConfig, executor);
            //     break;
            case External:
                result = new ExternalAdapter(testConfig, executor);
                break;
            case BookKeeper:
                result = new BookKeeperAdapter(testConfig, builderConfig.getConfig(BookKeeperConfig::builder), executor);
                break;
            default:
                throw new UnsupportedOperationException("Cannot create a StoreAdapter for TestType " + testConfig.getTestType());
        }
        return result;
    }

    //endregion

    //region Feature

    public enum Feature {
        CreateStream,
        DeleteStream,
        Append,
        SealStream,
        TailRead,
        RandomRead,
        Transaction,
        StorageDirect,
        Tables;
        public void ensureSupported(StoreAdapter storeAdapter, String operationName) {
            if (!storeAdapter.isFeatureSupported(this)) {
                throw new UnsupportedOperationException(String.format("Cannot %s because StoreAdapter does not support '%s'.", operationName, this));
            }
        }
    }

    //endregion
}
