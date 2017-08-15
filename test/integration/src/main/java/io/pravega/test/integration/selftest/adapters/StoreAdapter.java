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
import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import io.pravega.test.integration.selftest.TestLogger;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstraction layer for Pravega operations that are valid from the Self Tester.
 */
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

    //region Operations

    /**
     * Appends the given Event.
     *
     * @param streamName The name of the Stream to append to.
     * @param event      The Event to append.
     * @param timeout    Timeout for the operation.
     * @return A CompletableFuture that will be completed when the Event is appended.
     */
    public abstract CompletableFuture<Void> append(String streamName, Event event, Duration timeout);

    /**
     * Creates a new StoreReader that can read from this Store.
     * @return A new instance of a class implementing StoreReader.
     */
    public abstract StoreReader createReader();

    /**
     * Creates a new Stream.
     * @param streamName The name of the Stream to create.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that will be completed when the operation is complete.
     */
    public abstract CompletableFuture<Void> createStream(String streamName, Duration timeout);

    /**
     * Creates a new Transaction.
     * @param parentStream The Stream on which to create a transaction.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that will be completed when the operation is complete and will contain the name of the
     * Transaction.
     */
    public abstract CompletableFuture<String> createTransaction(String parentStream, Duration timeout);

    /**
     * Merges a Transaction.
     *
     * @param transactionName The Transaction to merge.
     * @param timeout         Timeout for the operation.
     * @return A CompletableFuture that will be completed when the operation is complete.
     */
    public abstract CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout);

    /**
     * Aborts a Transaction.
     *
     * @param transactionName The Transaction to abort.
     * @param timeout         Timeout for the operation.
     * @return A CompletableFuture that will be completed when the operation is complete.
     */
    public abstract CompletableFuture<Void> abortTransaction(String transactionName, Duration timeout);

    /**
     * Seals a Stream.
     *
     * @param streamName The Stream to seal.
     * @param timeout    Timeout for the operation.
     * @return A CompletableFuture that will be completed when the operation is complete.
     */
    public abstract CompletableFuture<Void> seal(String streamName, Duration timeout);

    /**
     * Deletes a Stream.
     *
     * @param streamName The Stream to delete.
     * @param timeout    Timeout for the operation.
     * @return A CompletableFuture that will be completed when the operation is complete.
     */
    public abstract CompletableFuture<Void> delete(String streamName, Duration timeout);

    /**
     * Gets a Snapshot of the SegmentStore thread pool.
     *
     * @return The Snapshot, or null if no such information is available.
     */
    public abstract ExecutorServiceHelpers.Snapshot getStorePoolSnapshot();

    /**
     * Determines whether the given Feature is supported or not.
     *
     * @param feature The feature to check.
     * @return True if supported, false otherwise.
     */
    public abstract boolean isFeatureSupported(Feature feature);


    protected void ensureRunning() {
        Preconditions.checkState(state() == State.RUNNING, "%s is not running.", logId);
    }

    protected void log(String messageFormat, Object... args) {
        TestLogger.log(this.logId, messageFormat, args);
    }

    //endregion

    //region Factory

    /**
     * Creates a new instance of the StoreAdapter using the given configurations.
     *
     * @param testConfig    The TestConfig to use. The TestType from this config will be used to determine what kind of
     *                      instance to create.
     * @param builderConfig A ServiceBuilderConfig to use for the SegmentStore.
     * @param executor      An Executor to use for test-related async operations.
     * @return The created StoreAdapter Instance.
     */
    public static StoreAdapter create(TestConfig testConfig, ServiceBuilderConfig builderConfig, ScheduledExecutorService executor) {
        StoreAdapter result;
        switch (testConfig.getTestType()) {
            case SegmentStore:
                result = new SegmentStoreAdapter(testConfig, builderConfig, executor);
                break;
            case InProcessMock:
                result = new InProcessMockClientAdapter(testConfig, executor);
                break;
            case InProcessStore:
                result = new InProcessListenerWithRealStoreAdapter(testConfig, builderConfig, executor);
                break;
            case OutOfProcess:
                result = new OutOfProcessAdapter(testConfig, builderConfig, executor);
                break;
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

    /**
     * Defines various Features that can be supported by an implementation of this interface.
     */
    public enum Feature {
        /**
         * Creating new Streams.
         */
        Create,
        /**
         * Deleting Streams.
         */
        Delete,

        /**
         * Appending Events.
         */
        Append,

        /**
         * Sealing Streams.
         */
        Seal,

        /**
         * Tail-Reading from Streams.
         */
        TailRead,

        /**
         * Reading at random positions from Streams.
         */
        RandomRead,

        /**
         * Transactions.
         */
        Transaction,

        /**
         * Direct Storage Access.
         */
        StorageDirect;

        /**
         * Ensures that the given StoreAdapter supports the given operation name.
         *
         * @param storeAdapter  The StoreAdapter to query.
         * @param operationName The name of the operation (enum value) to check.
         * @throws UnsupportedOperationException If the operation is not supported.
         */
        public void ensureSupported(StoreAdapter storeAdapter, String operationName) {
            if (!storeAdapter.isFeatureSupported(this)) {
                throw new UnsupportedOperationException(String.format("Cannot %s because StoreAdapter does not support '%s'.", operationName, this));
            }
        }
    }

    //endregion
}
