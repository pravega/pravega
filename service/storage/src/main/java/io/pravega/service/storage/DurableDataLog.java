/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.storage;

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.CloseableIterator;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Sequential Log that contains contiguous ranges of bytes.
 */
public interface DurableDataLog extends AutoCloseable {
    /**
     * Initializes the DurableDataLog and performs any recovery steps that may be required.
     *
     * @param timeout Timeout for the operation.
     * @throws DurableDataLogException When an exception occurred. This can be one of the following:
     *                                 DataLogNotAvailableException: it is not possible to reach the DataLog at
     *                                 the current time;
     *                                 DataLogWriterNotPrimaryException: the DurableDataLog could not acquire
     *                                 the exclusive write lock for its log;
     *                                 DataLogInitializationException: a general initialization failure occurred.
     */
    void initialize(Duration timeout) throws DurableDataLogException;

    /**
     * Adds a new entry to the log.
     * The exceptions that can be returned in the CompletableFuture (asynchronously) can be:
     * <ul>
     * <li>DataLogNotAvailableException - When it is not possible to write to the DataLog at the current time.
     * <li>DataLogWriterNotPrimaryException - When the DurableDataLog has lost the exclusive write lock for its log.
     * <li>WriteFailureException - When a general failure occurred with the write.
     * <li>WriteTooLongException - When a write that is greater than getMaxAppendLength() is given.
     * </ul>
     *
     * @param data    An ArrayView representing the data to append.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the LogAddress within the log for the entry. If the entry
     * failed to be added, this Future will complete with the appropriate exception.
     */
    CompletableFuture<LogAddress> append(ArrayView data, Duration timeout);

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
     */
    CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout);

    /**
     * Reads all the entries in the log.
     *
     * @return A CloseableIterator with the result.
     * @throws DataLogNotAvailableException: is not possible to reach the DataLog at the current time;
     * @throws DurableDataLogException:      the operation was unable to open a reader.
     * @throws DurableDataLogException       If another kind of exception occurred.
     */
    CloseableIterator<ReadItem, DurableDataLogException> getReader() throws DurableDataLogException;

    /**
     * Gets the maximum number of bytes allowed for a single append.
     */
    int getMaxAppendLength();

    /**
     * Gets a value indicating the Sequence of the last data that was committed. This is the value returned by
     * the last call to append().
     *
     * @return The requested value, or -1 if the information is unknown.
     */
    long getLastAppendSequence();

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