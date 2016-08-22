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
     * Reads a range of bytes from the StreamSegment.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param offset            The offset in the StreamSegment to read data from.
     * @param buffer            A buffer to use for reading data.
     * @param bufferOffset      The offset in the buffer to start writing data to.
     * @param length            The number of bytes to read.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the number of bytes read. If the operation failed,
     * it will contain the cause of the failure.
     * @throws ArrayIndexOutOfBoundsException If bufferOffset is invalid for the buffer.
     * @throws ArrayIndexOutOfBoundsException If bufferOffset + length is invalid for the buffer.
     */
    CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout);

    /**
     * Gets current information about a StreamSegment.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the information requested about the StreamSegment.
     * If the operation failed, it will contain the cause of the failure.
     */
    CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout);
}
