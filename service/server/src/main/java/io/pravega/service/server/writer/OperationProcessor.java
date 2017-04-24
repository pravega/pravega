/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.writer;

/**
 * Defines a general Processor for Operations.
 */
interface OperationProcessor {
    /**
     * Gets a value indicating whether the SegmentAggregator is closed (for any kind of operations).
     */
    boolean isClosed();

    /**
     * Gets the SequenceNumber of the first operation that is not fully committed to Storage.
     */
    long getLowestUncommittedSequenceNumber();
}
