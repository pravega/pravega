package com.emc.pravega.service.server.logs;

import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ReadIndex;
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
     * @param cacheKey  The cache key, if any, for the operation. This only needs to be supplied if the operation is a
     *                  StreamSegmentAppendOperation.
     */
    void addToReadIndex(StorageOperation operation, CacheKey cacheKey) {
        if (operation instanceof StreamSegmentAppendOperation) {
            // Record an beginMerge operation.
            Preconditions.checkArgument(cacheKey.isInCache(), "key must be inserted into the cache prior to adding StreamSegmentAppendOperation to the ReadIndex");
            StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) operation;
            this.readIndex.append(cacheKey, appendOperation.getData().length);
        }

        if (operation instanceof MergeBatchOperation) {
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
