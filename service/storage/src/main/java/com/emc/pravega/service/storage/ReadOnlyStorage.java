/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.storage;

import com.emc.pravega.service.contracts.SegmentProperties;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Read-Only abstraction for Permanent Storage.
 */
public interface ReadOnlyStorage {
    /**
     * Attempts to open the given Segment and make it available for use for this instance of the Storage adapter.
     * This can be accomplished in a number of different ways based on the actual implementation of the ReadOnlyStorage
     * interface, but it can be compared to acquiring an exclusive lock on the given segment).
     *
     * @param streamSegmentName Name of the StreamSegment to be opened.
     * @return A CompletableFuture that, when completed, will indicate that the StreamSegment has been opened
     * If the operation failed, it will be failed with the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     */
    CompletableFuture<Void> open(String streamSegmentName);

    /**
     * Reads a range of bytes from the StreamSegment.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param offset            The offset in the StreamSegment to read data from.
     * @param buffer            A buffer to use for reading data.
     * @param bufferOffset      The offset in the buffer to start writing data to.
     * @param length            The number of bytes to read.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the number of bytes read. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     * @throws ArrayIndexOutOfBoundsException If bufferOffset or bufferOffset + length are invalid for the buffer.
     */
    CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout);

    /**
     * Gets current information about a StreamSegment.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the information requested about the StreamSegment.
     * If the operation failed, it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     */
    CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout);

    /**
     * Determines whether the given StreamSegment exists or not.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the information requested. If the operation failed,
     * it will contain the cause of the failure.
     */
    CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout);
}
