package com.emc.logservice.storageabstraction;

import com.emc.logservice.common.AsyncIterator;

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
     * @param timeout
     * @return A CompletableFuture that, when completed, will indicate that the operation has completed. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Void> initialize(Duration timeout);

    /**
     * Adds a new entry to the log.
     *
     * @param data    An InputStream representing the data to append. The InputStream must be positioned at the first byte
     *                where the data should be read from. The InputStream's available() method must also specify the number
     *                of bytes to append.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the Sequence within the log for the entry. If the entry
     * failed to be added, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Long> append(InputStream data, Duration timeout);

    /**
     * Truncates the log up to the given sequence.
     *
     * @param upToSequence The Sequence up to where to truncate. This is the value returned either by append() or obtained
     *                     via read().
     * @param timeout      The timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the truncation completed. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Void> truncate(long upToSequence, Duration timeout);

    /**
     * Reads a number of entries from the log.
     *
     * @param afterSequence The Sequence of the last entry before the first one to read.
     * @return An AsyncIterator with the result.
     * @throws DurableDataLogException If the operation was unable to open a reader.
     */
    AsyncIterator<ReadItem> getReader(long afterSequence) throws DurableDataLogException;

    /**
     * Gets the maximum number of bytes allowed for a single append.
     *
     * @return The result.
     */
    int getMaxAppendLength();

    /**
     * Gets a value indicating the start offset of the last data that was committed. This is the value returned by
     * the last call to append().
     *
     * @return The requested value, or -1 if the information is unknown.
     */
    long getLastAppendSequence();

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
         * Gets the payload associated with this ReadItem.
         *
         * @return
         */
        byte[] getPayload();

        /**
         * Gets a value indicating the Sequence within the Log that this ReadItem exists at.
         *
         * @return
         */
        long getSequence();
    }
}