package com.emc.logservice.Logs;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an ordered log of entries.
 *
 * @param <TEntry>    The type of the entries.
 * @param <TSequence> The type of the sequence for each entry.
 */
public interface SequentialLog<TEntry, TSequence> {
    /**
     * Adds a new entry to the log.
     *
     * @param entry   The entry to add.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the Sequence for the entry. If the entry failed to
     * be added, this Future will complete with the appropriate exception.
     */
    CompletableFuture<TSequence> add(TEntry entry, Duration timeout);

    /**
     * Truncates the log up to the given sequence.
     *
     * @param upToSequence The Sequence up to where to truncate.
     * @param timeout      The timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the truncation completed. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Void> truncate(TSequence upToSequence, Duration timeout);

    /**
     * Reads a number of entries from the log.
     *
     * @param afterSequence The Sequence of the last entry before the first one to read.
     * @param maxCount      The maximum number of entries to read.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain an Iterator with the result. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Iterator<TEntry>> read(TSequence afterSequence, int maxCount, Duration timeout);
}

