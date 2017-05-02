/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.segmentstore.storage;

import io.pravega.server.segmentstore.contracts.SegmentProperties;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an abstraction for Permanent Storage.
 */
public interface Storage extends ReadOnlyStorage, AutoCloseable {
    /**
     * Attempts to open the given Segment in read-write mode and make it available for use for this instance of the Storage
     * adapter.
     * A single active read-write SegmentHandle can exist at any given time for a particular Segment, regardless of owner,
     * while a read-write SegmentHandle can coexist with any number of read-only SegmentHandles for that Segment (obtained
     * by calling openRead()).
     * This can be accomplished in a number of different ways based on the actual implementation of the Storage
     * interface, but it can be compared to acquiring an exclusive lock on the given segment).
     *
     * @param streamSegmentName Name of the StreamSegment to be opened.
     * @return A CompletableFuture that, when completed, will contain a read-write SegmentHandle that can be used to access
     * the segment for read and write activities (ex: read, get, write, seal, concat). If the operation failed, it will be
     * failed with the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     */
    CompletableFuture<SegmentHandle> openWrite(String streamSegmentName);

    /**
     * Creates a new StreamSegment in this Storage Layer.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the StreamSegment has been created (it will
     * contain a StreamSegmentInformation for a blank stream). If the operation failed, it will contain the cause of the
     * failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentExistsException: When the given Segment already exists in Storage.
     * </ul>
     */
    CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout);

    /**
     * Writes the given data to the StreamSegment.
     *
     * @param handle  A read-write SegmentHandle that points to a Segment to write to.
     * @param offset  The offset in the StreamSegment to write data at.
     * @param data    An InputStream representing the data to write.
     * @param length  The length of the InputStream.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> BadOffsetException: When the given offset does not match the actual length of the segment in storage.
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * <li> StorageNotPrimaryException: When this Storage instance is no longer primary for this Segment (it was fenced out).
     * </ul>
     * @throws IllegalArgumentException If handle is read-only.
     */
    CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout);

    /**
     * Seals a StreamSegment. No further modifications are allowed on the StreamSegment after this operation completes.
     *
     * @param handle  A read-write SegmentHandle that points to a Segment to Seal.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed. If the operation
     * failed, it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * <li> StorageNotPrimaryException: When this Storage instance is no longer primary for this Segment (it was fenced out).
     * </ul>
     * @throws IllegalArgumentException If handle is read-only.
     */
    CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout);

    /**
     * Concatenates two StreamSegments together. The Source StreamSegment will be appended as one atomic block at the end
     * of the Target StreamSegment (but only if its length equals the given offset), after which the Source StreamSegment
     * will cease to exist. Prior to this operation, the Source StreamSegment must be sealed.
     *
     * @param targetHandle  A read-write SegmentHandle that points to the Target StreamSegment. After this operation
     *                      is complete, this is the surviving StreamSegment.
     * @param offset        The offset in the Target StreamSegment to concat at.
     * @param sourceSegment The Source StreamSegment. This StreamSegment will be concatenated to the Target StreamSegment.
     *                      After this operation is complete, this StreamSegment will no longer exist.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> BadOffsetException: When the given offset does not match the actual length of the target segment in storage.
     * <li> StreamSegmentNotExistsException: When the either the source Segment or the target Segment do not exist in Storage.
     * <li> StorageNotPrimaryException: When this Storage instance is no longer primary for the target Segment (it was fenced out).
     * <li> IllegalStateException: When the Source Segment is not Sealed.
     * </ul>
     * @throws IllegalArgumentException If targetHandle is read-only.
     */
    CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout);

    /**
     * Deletes a StreamSegment.
     *
     * @param handle  A read-write SegmentHandle that points to a Segment to Delete.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * <li> StorageNotPrimaryException: When this Storage instance is no longer primary for this Segment (it was fenced out).
     * </ul>
     * @throws IllegalArgumentException If handle is read-only.
     */
    CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout);

    @Override
    void close();
}
