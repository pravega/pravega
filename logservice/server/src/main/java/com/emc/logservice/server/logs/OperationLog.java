package com.emc.logservice.server.logs;

import com.emc.logservice.server.Container;
import com.emc.logservice.server.logs.operations.Operation;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Sequential Log made of Log Operations.
 */
public interface OperationLog extends Container {
    /**
     * Adds a new Operation to the log.
     *
     * @param operation The Operation to append.
     * @param timeout   Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the Sequence for the Operation. If the entry failed to
     * be added, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Long> add(Operation operation, Duration timeout);

    /**
     * Truncates the log up to the given sequence.
     *
     * @param upToSequence The Sequence up to where to truncate.
     * @param timeout      The timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the truncation completed. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Void> truncate(long upToSequence, Duration timeout);

    /**
     * Reads a number of entries from the log.
     *
     * @param afterSequence The Sequence of the last entry before the first one to getReader.
     * @param maxCount      The maximum number of entries to getReader.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain an Iterator with the result. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Iterator<Operation>> read(long afterSequence, int maxCount, Duration timeout);
}

