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

package com.emc.pravega.service.server.writer;

import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.server.logs.operations.Operation;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Data Source for a StorageWriter
 */
public interface WriterDataSource {
    /**
     * Acknowledges that all Operations with Sequence Numbers up to and including the given one have been successfully committed.
     *
     * @param upToSequence The Sequence up to where to acknowledge.
     * @param timeout      The timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the acknowledgment completed. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Void> acknowledge(long upToSequence, Duration timeout);

    /**
     * Reads a number of entries from the Data Source.
     *
     * @param afterSequence The Sequence of the last entry before the first one to read.
     * @param maxCount      The maximum number of entries to read.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain an Iterator with the result. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Iterator<Operation>> read(long afterSequence, int maxCount, Duration timeout);

    /**
     * Indicates that the given sourceStreamSegmentId is merged into the given targetStreamSegmentId.
     *
     * @param targetStreamSegmentId The Id of the StreamSegment to merge into.
     * @param sourceStreamSegmentId The Id of the StreamSegment to merge.
     */
    void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId);

    /**
     * Gets the contents of a CacheKey from the cache.
     *
     * @param key The key to search by.
     * @return The payload associated with the key, or null if no such entry exists.
     */
    byte[] get(CacheKey key);
}
