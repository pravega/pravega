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
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;

/**
 * Provides methods for updating the Cache and the ReadIndex, as a result of the DurableLog processing operations.
 */
@Slf4j
public class CacheUpdater {
    //region Members

    private final Cache cache;
    private final ReadIndex readIndex;
    private HashSet<Long> recentStreamSegmentIds;
    private final String traceObjectId;

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

        this.traceObjectId = String.format("CacheUpdater[%s]", cache.getId());
        this.cache = cache;
        this.readIndex = readIndex;
        this.recentStreamSegmentIds = new HashSet<>();
    }

    //endregion

    /**
     * Adds the contents of the given StreamSegmentAppendOperation to the cache.
     *
     * @param operation The operation to add.
     * @return The CacheKey that is associated with the given operation cache entry.
     * @throws IllegalArgumentException If the operation does not have a proper Offset set.
     */
    CacheKey addToCache(StreamSegmentAppendOperation operation) {
        CacheKey key = new CacheKey(operation.getStreamSegmentId(), operation.getStreamSegmentOffset());
        this.cache.insert(key, operation.getData());
        assert key.isInCache() : "Key does not have InCache flag set after insertion into the cache: " + key;
        return key;
    }

    /**
     * Removes the data associated with the given CacheKey from the cache.
     *
     * @param key The key to use.
     * @return True if any data was removed, false otherwise.
     */
    boolean removeFromCache(CacheKey key) {
        return this.cache.remove(key);
    }

    /**
     * Registers the given operation in the ReadIndex.
     *
     * @param operation The operation to register.
     */
    void addToReadIndex(StorageOperation operation) {
        if (operation instanceof CachedStreamSegmentAppendOperation) {
            // Record a CachedStreamSegmentAppendOperation. This has all the info required to properly record an append
            // in the ReadIndex.
            CachedStreamSegmentAppendOperation appendOperation = (CachedStreamSegmentAppendOperation) operation;
            Preconditions.checkArgument(appendOperation.getCacheKey().isInCache(), "key must be inserted into the cache prior to adding CachedStreamSegmentAppendOperation to the ReadIndex");
            this.readIndex.append(appendOperation.getCacheKey(), appendOperation.getLength());
        } else if (operation instanceof StreamSegmentAppendOperation) {
            // Record a StreamSegmentAppendOperation. Just in case, we also support this type of operation, but we need to
            // log a warning indicating so. This means we do not optimize memory properly, and we end up storing data
            // in two different places.
            log.warn("{}: Unexpected operation encountered: {}", this.traceObjectId, operation);
            StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) operation;
            CacheKey key = addToCache(appendOperation);
            this.readIndex.append(key, appendOperation.getData().length);
        } else if (operation instanceof MergeBatchOperation) {
            // Record a Merge Batch operation. We call beginMerge here, and the LogSynchronizer will call completeMerge.
            MergeBatchOperation mergeOperation = (MergeBatchOperation) operation;
            this.readIndex.beginMerge(mergeOperation.getStreamSegmentId(), mergeOperation.getTargetStreamSegmentOffset(), mergeOperation.getBatchStreamSegmentId());
        }

        // Record recent activity on stream segment, if applicable.
        // We should record this for any kind of StorageOperation. When we issue 'triggerFutureReads' on the readIndex,
        // it should include 'sealed' StreamSegments too - any Future Reads waiting on that Offset will be cancelled.
        synchronized (this.readIndex) {
            this.recentStreamSegmentIds.add(operation.getStreamSegmentId());
        }
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
