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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Read-Only abstraction for Permanent Storage.
 */
public interface ReadOnlyStorage {
    /**
     * Attempts to acquire an exclusive lock for this Segment. If successful, a SegmentHandle will be returned, which
     * will have to be used for all operations that access this segment. This lock is owned by this instance of the
     * Storage adapter and cannot be used by another one, whether in the same process or externally.
     *
     * @param streamSegmentName Name of the StreamSegment for which to acquire a lock.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the StreamSegment has been opened,
     * an exclusive lock acquired for it and will contain a SegmentHandle that can be used to operate on the Segment.
     * If the operation failed, it will be failed with the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * <li> TODO: StorageWriterNotPrimaryException: Unable to acquire the lock.
     * </ul>
     */
    CompletableFuture<SegmentHandle> open(String streamSegmentName, Duration timeout);

    /**
     * Reads a range of bytes from the StreamSegment.
     *
     * @param segmentHandle A SegmentHandle identifying the Segment to read from.
     * @param offset        The offset in the StreamSegment to read data from.
     * @param buffer        A buffer to use for reading data.
     * @param bufferOffset  The offset in the buffer to start writing data to.
     * @param length        The number of bytes to read.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the number of bytes read. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * <li> TODO: StorageWriterNotPrimaryException: Exclusive lock for this Segment is no longer owned by this instance.
     * </ul>
     * @throws ArrayIndexOutOfBoundsException If bufferOffset or bufferOffset + length are invalid for the buffer.
     */
    CompletableFuture<Integer> read(SegmentHandle segmentHandle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout);

    /**
     * Gets current information about a StreamSegment.
     *
     * @param segmentHandle A SegmentHandle identifying the Segment to get info.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the information requested about the StreamSegment.
     * If the operation failed, it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * <li> TODO: StorageWriterNotPrimaryException: Exclusive lock for this Segment is no longer owned by this instance.
     * </ul>
     */
    CompletableFuture<SegmentProperties> getStreamSegmentInfo(SegmentHandle segmentHandle, Duration timeout);

    /**
     * Determines whether the given StreamSegment exists or not.
     *
     * @param segmentHandle A SegmentHandle identifying the Segment to operate on.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the information requested. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> TODO: StorageWriterNotPrimaryException: Exclusive lock for this Segment is no longer owned by this instance.
     * </ul>
     */
    CompletableFuture<Boolean> exists(SegmentHandle segmentHandle, Duration timeout);
}
