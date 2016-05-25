package com.emc.logservice.storageabstraction;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Sequential Log that contains contiguous ranges of bytes.
 */
public interface DurableDataLog {

    /**
     * Adds a new entry to the log.
     *
     * @param data    The data to append.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the Sequence within the log for the entry. If the entry
     * failed to be added, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Long> append(byte[] data, Duration timeout);

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
     * @param maxCount      The maximum number of entries to read.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain an Iterator with the result. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Iterator<ReadItem>> read(long afterSequence, int maxCount, Duration timeout);

    /**
     * Gets the maximum number of bytes allowed for a single append.
     *
     * @return The result.
     */
    int getMaxAppendLength();

    /**
     * Gets a value indicating the start offset of the last data that was committed. This is the value returned by
     * the last call to append().
     * @return The requested value, or -1 if the information is unknown.
     */
    long getLastAppendSequence();

    /**
     * Performs any recovery steps required for this log.
     *
     * @param timeout
     * @return
     */
    CompletableFuture<Void> recover(Duration timeout);

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