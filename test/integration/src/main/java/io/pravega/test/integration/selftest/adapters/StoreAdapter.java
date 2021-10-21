/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    //region Stream Operations

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
    public abstract CompletableFuture<Void> sealStream(String streamName, Duration timeout);

    /**
     * Deletes a Stream.
     *
     * @param streamName The Stream to delete.
     * @param timeout    Timeout for the operation.
     * @return A CompletableFuture that will be completed when the operation is complete.
     */
    public abstract CompletableFuture<Void> deleteStream(String streamName, Duration timeout);

    //endregion

    //region Table Operations

    /**
     * Creates a new Table.
     *
     * @param tableName The name of the Table to create.
     * @param timeout   Timeout for the operation.
     * @return A CompletableFuture that will be completed when the Table has been created.
     */
    public abstract CompletableFuture<Void> createTable(String tableName, Duration timeout);

    /**
     * Deletes an existing Table.
     *
     * @param tableName The name of the Table to delete.
     * @param timeout   Timeout for the operation.
     * @return A CompletableFuture that will be completed when the Table has been deleted.
     */
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
            case SegmentStoreTable:
                result = new SegmentStoreAdapter(testConfig, builderConfig, executor);
                break;
            case AppendProcessor:
                result = new AppendProcessorAdapter(testConfig, builderConfig, executor);
                break;
            case InProcessMock:
            case InProcessMockTable:
                result = new InProcessMockClientAdapter(testConfig, executor);
                break;
            case InProcessStore:
            case InProcessStoreTable:
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
        CreateStream,

        /**
         * Deleting Streams.
         */
        DeleteStream,

        /**
         * Appending Events.
         */
        Append,

        /**
         * Sealing Streams.
         */
        SealStream,

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
        StorageDirect,

        /**
         * Table Operations, such as Put/ConditionalPut, Remove/ConditionalRemove, Get, Iterators.
         */
        Tables;

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
