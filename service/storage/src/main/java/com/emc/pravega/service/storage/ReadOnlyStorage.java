/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
     * Initializes this Storage instance with the given ContainerEpoch.
     *
     * @param containerEpoch The Container Epoch to initialize with.
     */
    void initialize(long containerEpoch);

    /**
     * Opens the given Segment in read-only mode without acquiring any locks or blocking on any existing write-locks and
     * makes it available for use for this instance of Storage.
     * Multiple read-only Handles can coexist at any given time and allow concurrent read-only access to the Segment,
     * regardless of whether there is another non-read-only SegmentHandle that modifies the segment at that time.
     *
     * @param streamSegmentName Name of the StreamSegment to be opened in read-only mode.
     * @return A CompletableFuture that, when completed, will contain a read-only SegmentHandle that can be used to
     * access the segment for non-modify activities (ex: read, get). If the operation failed, it will be failed with the
     * cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     */
    CompletableFuture<SegmentHandle> openRead(String streamSegmentName);

    /**
     * Reads a range of bytes from the StreamSegment.
     *
     * @param handle       A SegmentHandle (read-only or read-write) that points to a Segment to read from.
     * @param offset       The offset in the StreamSegment to read data from.
     * @param buffer       A buffer to use for reading data.
     * @param bufferOffset The offset in the buffer to start writing data to.
     * @param length       The number of bytes to read.
     * @param timeout      Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the number of bytes read. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     * @throws ArrayIndexOutOfBoundsException If bufferOffset or bufferOffset + length are invalid for the buffer.
     */
    CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout);

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
