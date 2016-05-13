package com.emc.logservice.Logs;

import com.emc.logservice.*;
import com.emc.logservice.Logs.Operations.*;

import java.util.HashSet;

/**
 * Helper class that allows appending Log Operations to available InMemory Structures.
 */
public class MemoryLogUpdater {
    //region Private

    private final StreamSegmentCache cache;
    private final MemoryOperationLog inMemoryOperationLog;
    private HashSet<Long> recentStreamSegmentIds;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MemoryLogUpdater class.
     *
     * @param inMemoryOperationLog InMemory Operation Log.
     * @param cache                Cache.
     */
    public MemoryLogUpdater(MemoryOperationLog inMemoryOperationLog, StreamSegmentCache cache) {
        if (cache == null) {
            throw new NullPointerException("cache");
        }

        if (inMemoryOperationLog == null) {
            throw new NullPointerException("inMemoryOperationLog");
        }

        this.inMemoryOperationLog = inMemoryOperationLog;
        this.cache = cache;
        this.recentStreamSegmentIds = new HashSet<>();
    }

    //endregion

    //region Operations

    /**
     * Puts the Log Updater in Recovery Mode, using the given Metadata Source as interim.
     *
     * @param recoveryMetadataSource The metadata to use during recovery.
     */
    public void enterRecoveryMode(StreamSegmentMetadataSource recoveryMetadataSource) {
        this.cache.enterRecoveryMode(recoveryMetadataSource);
    }

    /**
     * Puts the Log Updater out of Recovery Mode, using the given Metadata Source as final.
     *
     * @param finalMetadataSource The Metadata to use after recovery.
     * @param success             Indicates whether recovery was successful. If not, the operations may be reverted and
     *                            the contents of the memory structures may be cleared out.
     */
    public void exitRecoveryMode(StreamSegmentMetadataSource finalMetadataSource, boolean success) {
        this.cache.exitRecoveryMode(finalMetadataSource, success);
    }

    /**
     * Appends the given operation.
     *
     * @param operation The operation to add.
     * @throws DataCorruptionException If a serious, non-recoverable, data corruption was detected, such as trying to
     *                                 add operations out of order.
     */
    public void add(Operation operation) throws DataCorruptionException {
        // Add entry to MemoryTransactionLog and ReadIndex. This callback is invoked from the QueueProcessor,
        // which always acks items in order of Sequence Number - so the entries should be ordered (but always check).
        if (!addToMemoryOperationLog(operation)) {
            // This is a pretty nasty one. It's safer to shut down the container than continue.
            // We either recorded the Operation correctly, but invoked this callback out of order, or we really
            // recorded the Operation in the wrong order (by sequence number). In either case, we will be inconsistent
            // while serving reads, so better stop now than later.
            throw new DataCorruptionException("About to have added a Log Operation to InMemoryOperationLog that was out of order.");
        }

        // Add entry to read index (if applicable).
        addToReadIndex(operation);
    }

    /**
     * Flushes recently appended items, if needed.
     * For example, it may trigger Future Reads on the Cache, if the cache supports that.
     */
    public void flush() {
        HashSet<Long> elements;
        synchronized (this.cache) {
            elements = this.recentStreamSegmentIds;
            this.recentStreamSegmentIds = new HashSet<>();
        }

        this.cache.triggerFutureReads(elements);
    }

    /**
     * Clears all in-memory structures of all data.
     *
     * @throws IllegalStateException If the operation cannot be performed due to the current state of the system, such
     *                               as metadata not being in Recovery mode.
     */
    public void clear() {
        this.cache.clear();
        this.inMemoryOperationLog.clear();
        synchronized (this.cache) {
            this.recentStreamSegmentIds = new HashSet<>();
        }
    }

    private boolean addToMemoryOperationLog(Operation operation) {
        return this.inMemoryOperationLog.addIf(operation, previous -> previous.getSequenceNumber() < operation.getSequenceNumber());
    }

    private void addToReadIndex(Operation operation) {
        if (operation instanceof StorageOperation) {
            if (operation instanceof StreamSegmentAppendOperation) {
                // Record an beginMerge operation.
                StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) operation;
                this.cache.append(appendOperation.getStreamSegmentId(), appendOperation.getStreamSegmentOffset(), appendOperation.getData());
            }

            if (operation instanceof MergeBatchOperation) {
                // Record a Merge Batch operation. We call beginMerge here, and the LogSynchronizer will call completeMerge.
                MergeBatchOperation mergeOperation = (MergeBatchOperation) operation;
                this.cache.beginMerge(mergeOperation.getStreamSegmentId(), mergeOperation.getTargetStreamSegmentOffset(), mergeOperation.getBatchStreamSegmentId(), mergeOperation.getBatchStreamSegmentLength());
            }

            // Record recent activity on stream segment, if applicable.
            // We should record this for any kind of StorageOperation. When we issue 'triggerFutureReads' on the cache,
            // it should include 'sealed' StreamSegments too - any Future Reads waiting on that Offset will be cancelled.
            synchronized (this.cache) {
                this.recentStreamSegmentIds.add(((StorageOperation) operation).getStreamSegmentId());
            }
        }
    }

    //endregion
}
