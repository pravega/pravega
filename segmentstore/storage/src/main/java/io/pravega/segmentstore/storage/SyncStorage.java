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

import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Defines an abstraction for Permanent Storage.
 * Note: not all operations defined here are needed in the (async) Storage interface.
 */
@SuppressWarnings("checkstyle:JavadocMethod")
public interface SyncStorage extends AutoCloseable {
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
     * @return A read-only SegmentHandle that can be used to access the segment for non-modify activities (ex: read).
     * @throws StreamSegmentNotExistsException If the Segment does not exist.
     */
    SegmentHandle openRead(String streamSegmentName) throws StreamSegmentException;

    /**
     * Reads a range of bytes from the StreamSegment.
     *
     * @param handle       A SegmentHandle (read-only or read-write) that points to a Segment to read from.
     * @param offset       The offset in the StreamSegment to read data from.
     * @param buffer       A buffer to use for reading data.
     * @param bufferOffset The offset in the buffer to start writing data to.
     * @param length       The number of bytes to read.
     * @return The number of bytes read. There is no guarantee that this value equals 'length'.
     * @throws ArrayIndexOutOfBoundsException  If bufferOffset or bufferOffset + length are invalid for the buffer.
     * @throws StreamSegmentNotExistsException If the Segment does not exist.
     */
    int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException;

    /**
     * Gets current information about a StreamSegment.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @return A SegmentProperties object with current information about the StreamSegment.
     * If the operation failed, it will contain the cause of the failure. Notable exceptions:
     * @throws StreamSegmentNotExistsException If the Segment does not exist.
     */
    SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentException;

    /**
     * Determines whether the given StreamSegment exists or not.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @return True if the Segment exists, false otherwise.
     */
    boolean exists(String streamSegmentName);

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
     * @return A read-write SegmentHandle that can be used to access the segment for read and write activities (ex: read,
     * write, seal, concat).If the segment is sealed, then a Read-Only handle is returned.
     * @throws StreamSegmentNotExistsException If the Segment does not exist.
     * @throws StorageNotPrimaryException      If this Storage instance is not a Primary writer for this Segment.
     */
    SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentException;

    /**
     * Creates a new StreamSegment.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @return A read-write SegmentHandle that can be used to access the segment for read and write activities (ex: read,
     * write, seal, concat).
     * @throws StreamSegmentException If an exception occurred.
     */
    SegmentHandle create(String streamSegmentName) throws StreamSegmentException;

    /**
     * Creates a new StreamSegment with given SegmentRollingPolicy.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param rollingPolicy     The Rolling Policy to apply to this StreamSegment.
     * @return A read-write SegmentHandle that can be used to access the segment for read and write activities (ex: read,
     * write, seal, concat).
     * @throws StreamSegmentException If an exception occurred.
     */
    default SegmentHandle create(String streamSegmentName, SegmentRollingPolicy rollingPolicy) throws StreamSegmentException {
        // By default this creates a blank Segment. This is the default behavior for Storage implementations that do not
        // support Segment Rolling.
        return create(streamSegmentName);
    }

    /**
     * Deletes a StreamSegment.
     *
     * @param handle A read-write SegmentHandle that points to a Segment to Delete.
     * @throws IllegalArgumentException        If targetHandle is read-only.
     * @throws StreamSegmentNotExistsException When the given Segment does not exist in Storage.
     * @throws StorageNotPrimaryException      When this Storage instance is no longer primary for this Segment (it was
     *                                         fenced out).
     */
    void delete(SegmentHandle handle) throws StreamSegmentException;

    /**
     * Writes the given data to the StreamSegment.
     *
     * @param handle A read-write SegmentHandle that points to a Segment to write to.
     * @param offset The offset in the StreamSegment to write data at.
     * @param data   An InputStream representing the data to write.
     * @param length The length of the InputStream.
     * @throws IllegalArgumentException        If handle is read-only.
     * @throws BadOffsetException              When the given offset does not match the actual length of the segment in
     *                                         Storage.
     * @throws StreamSegmentNotExistsException When the given Segment does not exist in Storage.
     * @throws StreamSegmentSealedException    When the given Segment is Sealed.
     * @throws StorageNotPrimaryException      When this Storage instance is no longer primary for this Segment (it was
     *                                         fenced out).
     */
    void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException;

    /**
     * Seals a StreamSegment. No further modifications are allowed on the StreamSegment after this operation completes.
     *
     * @param handle A read-write SegmentHandle that points to a Segment to Seal.
     * @throws IllegalArgumentException        If handle is read-only.
     * @throws StreamSegmentNotExistsException When the given Segment does not exist in Storage.
     * @throws StorageNotPrimaryException      When this Storage instance is no longer primary for this Segment (it was
     *                                         fenced out).
     */
    void seal(SegmentHandle handle) throws StreamSegmentException;

    /**
     * Un-Seals a StreamSegment. After this operation completes successfully, the Segment can be written to again..
     *
     * @param handle A read-only or read-write SegmentHandle that points to a Segment to Seal. Since open-write will only
     *               return a read-only handle for a Sealed Segment, this is the only modify operation that allows a
     *               read-only handle as input.
     * @throws StreamSegmentNotExistsException When the given Segment does not exist in Storage.
     * @throws StorageNotPrimaryException      When this Storage instance is no longer primary for this Segment (it was
     *                                         fenced out).
     */
    void unseal(SegmentHandle handle) throws StreamSegmentException;

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
     * @throws IllegalArgumentException        If targetHandle is read-only.
     * @throws IllegalStateException           When the Source Segment is not Sealed.
     * @throws BadOffsetException              When the given offset does not match the actual length of the segment in
     *                                         Storage.
     * @throws StreamSegmentNotExistsException When the either the Source or Target Segments do not exist in Storage.
     * @throws StreamSegmentSealedException    When the target Segment is Sealed.
     * @throws StorageNotPrimaryException      When this Storage instance is no longer primary for this Segment (it was
     *                                         fenced out).
     */
    void concat(SegmentHandle targetHandle, long offset, String sourceSegment) throws StreamSegmentException;

    /**
     * Truncates all data in the given StreamSegment prior to the given offset. This does not fill the truncated data
     * in the segment with anything, nor does it "shift" the remaining data to the beginning. After this operation is
     * complete, any attempt to access the truncated data will result in an exception.
     * <p>
     * Notes:
     * * Depending on implementation, this may not truncate at the exact offset. It may truncate at some point prior to
     * the given offset, but it will never truncate beyond the offset.
     *
     * @param handle A read-write SegmentHandle that points to a Segment to write to.
     * @param offset The offset in the StreamSegment to truncate to.
     * @throws IllegalArgumentException        If targetHandle is read-only.
     * @throws UnsupportedOperationException   If supportsTruncation() == false.
     * @throws StreamSegmentNotExistsException When the given Segment does not exist in Storage.
     * @throws StorageNotPrimaryException      When this Storage instance is no longer primary for this Segment (it was
     *                                         fenced out).
     */
    void truncate(SegmentHandle handle, long offset) throws StreamSegmentException;

    /**
     * Gets a value indicating whether this Storage implementation can truncate Segments.
     *
     * @return True or false.
     */
    boolean supportsTruncation();

    /**
     * Gets a value indicating whether this {@link SyncStorage} implementation can replace whole Segments with new contents.
     *
     * @return True or false.
     */
    default boolean supportsReplace() {
        return false;
    }

    /**
     * Replaces a Segment with the given contents.  Please refer to the actual implementing class for more details with
     * respect to behavior, atomicity and recovery mechanisms.
     *
     * @param segment A {@link SegmentHandle} representing the Segment to replace.
     * @param contents A {@link BufferView} representing the new contents of the Segment.
     * @throws StreamSegmentException An eror occured generally one of the below:
     * @throws StreamSegmentNotExistsException When the given Segment does not exist in Storage.
     * @throws StorageNotPrimaryException      When this Storage instance is no longer primary for this Segment (it was
     *                                         fenced out).
     * @throws UnsupportedOperationException   If {@link #supportsReplace()} returns false.
     */
    default void replace(SegmentHandle segment, BufferView contents) throws StreamSegmentException {
        throw new UnsupportedOperationException("replace() is not implemented");
    }

    /**
     * Returns a new {@link SyncStorage} instance for the same Storage type as this one, but with {@link #replace} support
     * enabled. If there is no such implementation, this instance is returned.
     *
     * @return Either this instance or a new {@link SyncStorage} instance based on this one that can perform replaces.
     */
    default SyncStorage withReplaceSupport() {
        return this;
    }

    /**
     * Lists all the segments stored on the storage device.
     *
     * @return Iterator that can be used to enumerate and retrieve properties of all the segments.
     */
    Iterator<SegmentProperties> listSegments() throws IOException;

    @Override
    void close();
}
