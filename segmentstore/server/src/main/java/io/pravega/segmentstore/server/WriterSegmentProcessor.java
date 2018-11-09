/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.server.logs.operations.Operation;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a general Segment Processor for the Storage Writer.
 */
public interface WriterSegmentProcessor extends AutoCloseable {

    @Override
    void close();

    /**
     * Gets a value indicating whether the Segment Processor is closed (for any kind of operations).
     */
    boolean isClosed();

    /**
     * Gets the SequenceNumber of the first operation that is not fully committed to Storage.
     * See {@link Operation#getSequenceNumber} for a definition of Sequence Number.
     */
    long getLowestUncommittedSequenceNumber();

    /**
     * Gets a value indicating whether a call to {@link #flush} is required given the current state of this Segment Processor.
     */
    boolean mustFlush();

    /**
     * Adds the given {@link SegmentOperation} to the Processor.
     *
     * @param operation the SegmentOperation to add.
     * @throws DataCorruptionException  If the validation of the given Operation indicates a possible data corruption in
     *                                  the code (offset gaps, out-of-order operations, etc.)
     * @throws IllegalArgumentException If the validation of the given Operation indicates a possible non-corrupting bug
     *                                  in the code.
     */
    void add(SegmentOperation operation) throws DataCorruptionException;

    /**
     * Flushes the contents of the Processor.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a summary of the flush operation. If any errors
     * occurred during the flush, the Future will be completed with the appropriate exception.
     */
    CompletableFuture<WriterFlushResult> flush(Duration timeout);
}
