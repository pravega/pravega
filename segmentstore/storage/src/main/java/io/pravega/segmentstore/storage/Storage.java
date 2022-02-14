/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage;

import io.pravega.segmentstore.contracts.SegmentProperties;
import java.io.InputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an abstraction for Permanent Storage with async operations.
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
     * the segment for read and write activities (ex: read, get, write, seal, concat).
     * If the segment is sealed, then a Read-Only handle is returned.
     *
     * If the operation failed, it will be failed with the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     */
    CompletableFuture<SegmentHandle> openWrite(String streamSegmentName);

    /**
     * Creates a new StreamSegment in this Storage Layer with an Infinite Rolling Policy (No Rolling).
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a read-write SegmentHandle that can be used to access
     * the segment for read and write activities (ex: read, get, write, seal, concat). If the operation failed, it will contain the cause of the
     * failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentExistsException: When the given Segment already exists in Storage.
     * </ul>
     */
    default CompletableFuture<SegmentHandle> create(String streamSegmentName, Duration timeout) {
        return create(streamSegmentName, SegmentRollingPolicy.NO_ROLLING, timeout);
    }

    /**
     * Creates a new StreamSegment in this Storage Layer with the given Rolling Policy.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param rollingPolicy     The Rolling Policy to apply to this StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a read-write SegmentHandle that can be used to access
     *      * the segment for read and write activities (ex: read, get, write, seal, concat). If the operation failed, it will contain the cause of the
     * failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentExistsException: When the given Segment already exists in Storage.
     * </ul>
     */
    CompletableFuture<SegmentHandle> create(String streamSegmentName, SegmentRollingPolicy rollingPolicy, Duration timeout);

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

    /**
     * Truncates all data in the given StreamSegment prior to the given offset. This does not fill the truncated data
     * in the segment with anything, nor does it "shift" the remaining data to the beginning. After this operation is
     * complete, any attempt to access the truncated data will result in an exception.
     * <p>
     * Notes:
     * * Depending on implementation, this may not truncate at the exact offset. It may truncate at some point prior to
     * the given offset, but it will never truncate beyond the offset.
     *
     * @param handle  A read-write SegmentHandle that points to a Segment to write to.
     * @param offset  The offset in the StreamSegment to truncate to.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     */
    CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout);

    /**
     * Gets a value indicating whether this Storage implementation can truncate Segments.
     *
     * @return True or false.
     */
    boolean supportsTruncation();

    /**
     * Determines whether this Storage implementation supports atomic writes. Supporting atomic writes means that calls
     * to {@link #write} will either make all the payload available for reading (via {@link #getStreamSegmentInfo} and
     * {@link #read}) or none, regardless of whether the underlying Storage binding supports that (e.g., FileSystem does not).
     *
     * If this returns true, then even a system crash or other failure will guarantee that, upon a recovery, the payload
     * will either be visible in its entirety or none at all.
     *
     * @return True if atomic writes are supported, false otherwise.
     */
    boolean supportsAtomicWrites();

    /**
     * Lists all the segments stored on the storage device.
     *
     * @return A CompletableFuture that, when completed, will contain an {@link Iterator} that can be used to enumerate and retrieve properties of all the segments.
     * If the operation failed, it will contain the cause of the failure.
     */
    CompletableFuture<Iterator<SegmentProperties>> listSegments();

    @Override
    void close();
}
