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
package io.pravega.segmentstore.storage;

import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeArrayView;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Sequential Log that contains contiguous ranges of bytes.
 */
public interface DurableDataLog extends AutoCloseable {
    /**
     * Initializes the DurableDataLog and performs any recovery steps that may be required. This method will succeed only
     * if the DurableDataLog is enabled. Disabled DurableDataLogs cannot be initialized or otherwise recovered from.
     *
     * @param timeout Timeout for the operation.
     * @throws DurableDataLogException When an exception occurred. This can be one of the following:
     *                                 DataLogNotAvailableException: it is not possible to reach the DataLog at
     *                                 the current time;
     *                                 DataLogWriterNotPrimaryException: the DurableDataLog could not acquire
     *                                 the exclusive write lock for its log;
     *                                 DataLogDisabledException: if the log is disabled.
     *                                 DataLogInitializationException: a general initialization failure occurred.
     */
    void initialize(Duration timeout) throws DurableDataLogException;

    /**
     * Enables the DurableDataLog, if it is currently disabled. The invocation of this method does not require a previous
     * call to initialize() as it is not expected that the Log be initialized - since initialization can only be successful
     * on an Enabled Log.
     *
     * @throws DurableDataLogException When an exception occurred. This can be one of the following:
     *                                 DataLogNotAvailableException: it is not possible to reach the DataLog at
     *                                 the current time;
     *                                 DataLogWriterNotPrimaryException: the DurableDataLog could not acquire
     *                                 the exclusive write lock for its log;
     *                                 DataLogInitializationException: a general initialization failure occurred.
     * @throws IllegalStateException   If the DurableDataLog is not currently disabled.
     */
    void enable() throws DurableDataLogException;

    /**
     * Disables the DurableDataLog, if it is currently enabled. The invocation of this method requires the DurableDataLog
     * to be initialized, since it must be the current owner of the Log; as such a previous call to initialized() is expected.
     * After this method completes successfully, the current instance of the DurableDataLog will be closed (equivalent to
     * calling close()). This is in order to properly cancel any ongoing operations on it.
     *
     * @throws DurableDataLogException When an exception occurred. Notable exceptions:
     *                                 DataLogWriterNotPrimaryException: if the DurableDataLog has lost the exclusive write
     *                                 lock for its log.
     * @throws IllegalStateException   If the DurableDataLog is not currently enabled.
     */
    void disable() throws DurableDataLogException;

    /**
     * Adds a new entry to the log. Multiple concurrent calls to this method are allowed.
     *
     * The exceptions that can be returned in the CompletableFuture (asynchronously) can be:
     * <ul>
     * <li>DataLogNotAvailableException - When it is not possible to write to the DataLog at the current time.
     * <li>DataLogWriterNotPrimaryException - When the DurableDataLog has lost the exclusive write lock for its log.
     * <li>WriteFailureException - When a general failure occurred with the write.
     * <li>WriteTooLongException - When a write that is greater than getMaxAppendLength() is given.
     * </ul>
     *
     * Ordering guarantees:
     * <ul>
     * <li> Log ordering is only guaranteed to be the same as this method's invocation order as long as it is called
     * sequentially (subsequent calls need to wait for the method to exit synchronously before re-invoking).
     * <li> If an append is completed, it can safely be assumed that all appends prior to that have also successfully
     * been completed.
     * <li> If an append failed, all subsequent appends will be failed as well and the DurableDataLog will close. An append
     * is not considered failed if the method throws a synchronous exception (which means the append got rejected); failure
     * is always reported when the CompletableFuture returned by this method is completed exceptionally.
     * </ul>
     *
     * @param data    A CompositeArrayView representing the data to append.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the LogAddress within the log for the entry. If the entry
     * failed to be added, this Future will complete with the appropriate exception.
     * @throws IllegalStateException If the DurableDataLog is not currently initialized (which implies being enabled).
     */
    CompletableFuture<LogAddress> append(CompositeArrayView data, Duration timeout);

    /**
     * Truncates the log up to the given sequence.
     * The exceptions that can be returned in the CompletableFuture (asynchronously) can be:
     * <ul>
     * <li>DataLogNotAvailableException - When it is not possible to write to the DataLog at the current time.
     * <li>DataLogWriterNotPrimaryException - When the DurableDataLog has lost the exclusive write lock for its log.
     * <li>WriteFailureException - When a general failure occurred with the write.
     * </ul>
     *
     * @param upToAddress The LogAddress up to where to truncate. This is the value returned either by append() or obtained
     *                    via read().
     * @param timeout     The timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed. If the operation failed,
     * this Future will complete with the appropriate exception.
     * @throws IllegalStateException If the DurableDataLog is not currently initialized (which implies being enabled).
     */
    CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout);

    /**
     * Reads all the entries in the log.
     *
     * @return A CloseableIterator with the result.
     * @throws DurableDataLogException When an exception occurred. This can be one of the following:
     *                                 DataLogNotAvailableException: is not possible to reach the DataLog at the
     *                                 current time;
     *                                 DurableDataLogException: the operation was unable to open a reader.
     * @throws IllegalStateException   If the DurableDataLog is not currently initialized (which implies being enabled).
     */
    CloseableIterator<ReadItem, DurableDataLogException> getReader() throws DurableDataLogException;

    /**
     * Gets a {@link WriteSettings} containing limitations for appends.
     * @return A new {@link WriteSettings} object.
     */
    WriteSettings getWriteSettings();

    /**
     * Fetch the metadata for this log.
     * @return the metadata persisted for this DurableDataLog.
     * @throws DataLogInitializationException any exception with ZK while fetching metadata.
     */
    ReadOnlyLogMetadata loadMetadata() throws DataLogInitializationException;

    /**
     * Gets a value indicating the current Epoch of this DurableDataLog.
     * <p>
     * An Epoch is a monotonically strictly number that changes (not necessarily incremented) every time the DurableDataLog
     * is successfully initialized. This usually corresponds to a successful lock acquisition of the underlying log resource,
     * thus fencing out any other existing instances holding that lock.
     * <p>
     * For example, if instance A has an epoch smaller than that of instance B, and both are competing on the same underlying
     * log, then it is safe to assume that B has acquired the lock later than A and A can no longer use the lock. Should A
     * re-acquire this exclusive lock, its Epoch would change to a number that is greater than B's.
     *
     * @return The current Epoch value. This value is set once after initialization and never changed during the lifetime
     * of this object.
     */
    long getEpoch();

    /**
     * Override the epoch in the log metadata. To be used in cases where we initialize container from storage
     * where we override the epoch in container metadata with epoch read from starage.
     * @param epoch epoch to be overriden
     * @throws  DurableDataLogException in case of exceptions while overriding.
     */
    void overrideEpoch(long epoch) throws DurableDataLogException;

    /**
     * Gets a QueueStats with information about the current state of the queue.
     *
     * @return The result.
     */
    QueueStats getQueueStatistics();

    /**
     * Registers a {@link ThrottleSourceListener} that will be invoked every time the internal queue state changes by having
     * added or removed from it.
     *
     * @param listener The {@link ThrottleSourceListener} to register. This listener will be unregistered when its
     *                 {@link ThrottleSourceListener#isClosed()} is determined to be true.
     */
    void registerQueueStateChangeListener(ThrottleSourceListener listener);

    /**
     * Closes this instance of a DurableDataLog and releases any resources it holds.
     */
    @Override
    void close();

    /**
     * Defines a single item in a Read Result.
     */
    interface ReadItem {
        /**
         * Gets an InputStream representing the payload associated with this ReadItem.
         */
        InputStream getPayload();

        /**
         * Gets a value representing the Length of this ReadItem.
         */
        int getLength();

        /**
         * Gets a value indicating the Address within the Log that this ReadItem exists at.
         */
        LogAddress getAddress();
    }
}