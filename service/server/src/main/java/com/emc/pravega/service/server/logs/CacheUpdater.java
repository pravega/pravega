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

package com.emc.pravega.service.server.logs;

import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.logs.operations.CachedStreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.storage.Cache;
import com.google.common.base.Preconditions;

import java.util.HashSet;

/**
 * Provides methods for updating the Cache and the ReadIndex, as a result of the DurableLog processing operations.
 */
public class CacheUpdater {
    //region Members

    private final Cache cache;
    private final ReadIndex readIndex;
    private HashSet<Long> recentStreamSegmentIds;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the CacheUpdater class.
     *
     * @param cache     The cache to use.
     * @param readIndex The ReadIndex to use.
     */
    public CacheUpdater(Cache cache, ReadIndex readIndex) {
        Preconditions.checkNotNull(cache, "cache");
        Preconditions.checkNotNull(readIndex, "readIndex");

        this.cache = cache;
        this.readIndex = readIndex;
        this.recentStreamSegmentIds = new HashSet<>();
    }

    //endregion

    /**
     * Registers the given operation in the ReadIndex.
     *
     * @param operation The operation to register.
     */
    CacheKey addToReadIndex(StorageOperation operation) {
        CacheKey result = null;
        if (operation instanceof StreamSegmentAppendOperation) {
            // Record a StreamSegmentAppendOperation. Just in case, we also support this type of operation, but we need to
            // log a warning indicating so. This means we do not optimize memory properly, and we end up storing data
            // in two different places.
            StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) operation;
            result = this.readIndex.append(appendOperation.getStreamSegmentId(), appendOperation.getStreamSegmentOffset(), appendOperation.getData());
        } else if (operation instanceof MergeBatchOperation) {
            // Record a Merge Batch operation. We call beginMerge here, and the StorageWriter will call completeMerge.
            MergeBatchOperation mergeOperation = (MergeBatchOperation) operation;
            this.readIndex.beginMerge(mergeOperation.getStreamSegmentId(), mergeOperation.getStreamSegmentOffset(), mergeOperation.getBatchStreamSegmentId());
        } else {
            assert !(operation instanceof CachedStreamSegmentAppendOperation) : "attempted to add a CachedStreamSegmentAppendOperation to the ReadIndex";
        }

        // Record recent activity on stream segment, if applicable.
        // We should record this for any kind of StorageOperation. When we issue 'triggerFutureReads' on the readIndex,
        // it should include 'sealed' StreamSegments too - any Future Reads waiting on that Offset will be cancelled.
        synchronized (this.readIndex) {
            this.recentStreamSegmentIds.add(operation.getStreamSegmentId());
        }

        return result;
    }

    /**
     * Puts the ReadIndex in Recovery Mode, using the given Metadata Source as interim.
     *
     * @param recoveryMetadataSource The metadata to use during recovery.
     */
    void enterRecoveryMode(ContainerMetadata recoveryMetadataSource) {
        this.readIndex.enterRecoveryMode(recoveryMetadataSource);
    }

    /**
     * Puts the ReadIndex out of Recovery Mode, using the given Metadata Source as final.
     *
     * @param successfulRecovery Indicates whether recovery was successful. If not, the operations may be reverted and
     *                           the contents of the memory structures may be cleared out.
     */
    void exitRecoveryMode(boolean successfulRecovery) throws DataCorruptionException {
        this.readIndex.exitRecoveryMode(successfulRecovery);
    }

    /**
     * Flushes recently appended items, if needed.
     * For example, it may trigger Future Reads on the ReadIndex, if the readIndex supports that.
     */
    void flush() {
        HashSet<Long> elements;
        synchronized (this.readIndex) {
            elements = this.recentStreamSegmentIds;
            this.recentStreamSegmentIds = new HashSet<>();
        }

        this.readIndex.triggerFutureReads(elements);
    }

    /**
     * Clears all in-memory structures of all data and resets the cache to empty.
     *
     * @throws IllegalStateException If the operation cannot be performed due to the current state of the system, such
     *                               as metadata not being in Recovery mode.
     */
    void clear() {
        this.readIndex.clear();
        this.cache.reset();
        synchronized (this.readIndex) {
            this.recentStreamSegmentIds = new HashSet<>();
        }
    }
}
