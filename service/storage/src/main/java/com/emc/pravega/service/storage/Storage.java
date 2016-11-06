/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.storage;

import com.emc.pravega.service.contracts.SegmentProperties;

import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an abstraction for Tier-2 Storage.
 */
public interface Storage extends ReadOnlyStorage, AutoCloseable {
    /**
     * Creates a new StreamSegment in this Storage Layer.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the StreamSegment has been created and contain
     * a SegmentHandle that can be used to operate on the Segment.. If the operation failed, it will be failed with the
     * cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentExistsException: When the given Segment already exists in Storage.
     * </ul>
     */
    CompletableFuture<SegmentHandle> create(String streamSegmentName, Duration timeout);

    /**
     * Writes the given data to the StreamSegment.
     *
     * @param segmentHandle A SegmentHandle identifying the Segment to write to.
     * @param offset        The offset in the StreamSegment to write data at.
     * @param data          An InputStream representing the data to write.
     * @param length        The length of the InputStream.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> BadOffsetException: When the given offset does not match the actual length of the segment in storage.
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * <li> TODO: StorageWriterNotPrimaryException: Exclusive lock for this Segment is no longer owned by this instance.
     * </ul>
     */
    CompletableFuture<Void> write(SegmentHandle segmentHandle, long offset, InputStream data, int length, Duration timeout);

    /**
     * Seals a StreamSegment. No further modifications are allowed on the StreamSegment after this operation completes.
     *
     * @param segmentHandle A SegmentHandle identifying the Segment to seal.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed (it will contain a
     * StreamSegmentInformation with the final state of the StreamSegment). If the operation failed, it will contain the
     * cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentSealedException: When the segment is already sealed in Storage.
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * <li> TODO: StorageWriterNotPrimaryException: Exclusive lock for this Segment is no longer owned by this instance.
     * </ul>
     */
    CompletableFuture<SegmentProperties> seal(SegmentHandle segmentHandle, Duration timeout);

    /**
     * Concatenates two StreamSegments together. The Source StreamSegment will be appended as one atomic block at the end
     * of the Target StreamSegment (but only if its length equals the given offset), after which the Source StreamSegment
     * will cease to exist. Prior to this operation, the Source StreamSegment must be sealed.
     *
     * @param targetSegmentHandle A SegmentHandle identifying the Target StreamSegment. After this operation is complete, this
     *                            is the surviving StreamSegment.
     * @param offset              The offset in the Target StreamSegment to concat at.
     * @param sourceSegmentHandle A SegmentHandle identifying the Source StreamSegment. This StreamSegment will be concatenated
     *                            to the Target StreamSegment. After this operation is complete, this StreamSegment
     *                            will be deleted.
     * @param timeout             Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> BadOffsetException: When the given offset does not match the actual length of the target segment in storage.
     * <li> StreamSegmentNotExistsException: When the either the source Segment or the target Segment do not exist in Storage.
     * <li> TODO: StorageWriterNotPrimaryException: Exclusive lock for either the source or target Segment is no longer owned by this instance.
     * </ul>
     */
    CompletableFuture<Void> concat(SegmentHandle targetSegmentHandle, long offset, SegmentHandle sourceSegmentHandle, Duration timeout);

    /**
     * Deletes a StreamSegment.
     *
     * @param segmentHandle A SegmentHandle identifying the Segment to delete.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * <li> TODO: StorageWriterNotPrimaryException: Exclusive lock for this Segment is no longer owned by this instance.
     * </ul>
     */
    CompletableFuture<Void> delete(SegmentHandle segmentHandle, Duration timeout);

    @Override
    void close();
}
