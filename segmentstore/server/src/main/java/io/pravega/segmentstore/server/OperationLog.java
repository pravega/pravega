/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Sequential Log made of Log Operations.
 */
public interface OperationLog extends Container {
    /**
     * Adds a new {@link Operation} to the {@link OperationLog} with a given {@link OperationPriority}.
     *
     * @param operation The {@link Operation} to append.
     * @param priority  Operation Priority.
     * @param timeout   Timeout for the {@link Operation}.
     * @return A CompletableFuture that, when completed, will indicate that the {@link Operation} has been durably added.
     * If the {@link Operation} failed to be added, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Void> add(Operation operation, OperationPriority priority, Duration timeout);

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
     * Creates and persists a Metadata Checkpoint.
     *
     * @param timeout The timeout for the operation.
     * @return The Sequence Number of the Metadata Checkpoint.
     */
    CompletableFuture<Long> checkpoint(Duration timeout);

    /**
     * Reads a number of entries from the log.
     *
     * @param afterSequence The Sequence of the last entry before the first one to read.
     * @param maxCount      The maximum number of entries to read.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain an Iterator with the result. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Iterator<Operation>> read(long afterSequence, int maxCount, Duration timeout);

    /**
     * Waits until the OperationLog enters an Online State.
     *
     * @return A CompletableFuture that, when completed, will indicate that the OperationLog is Online. If the OperationLog
     * is already Online, this Future will be already completed when returned. If the OperationLog encounters an exception
     * while attempting to start (including it shutting down), this Future will be completed with the appropriate exception.
     */
    CompletableFuture<Void> awaitOnline();
}

