/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.util.SequencedItemList;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.logs.operations.CachedStreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.google.common.base.Preconditions;

/**
 * Helper class that allows appending Log Operations to available InMemory Structures.
 */
class MemoryStateUpdater {
    //region Private

    private final CacheUpdater cacheUpdater;
    private final SequencedItemList<Operation> inMemoryOperationLog;
    private final Runnable flushCallback;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MemoryStateUpdater class.
     *
     * @param inMemoryOperationLog InMemory Operation Log.
     * @param cacheUpdater         Cache Updater.
     */
    MemoryStateUpdater(SequencedItemList<Operation> inMemoryOperationLog, CacheUpdater cacheUpdater) {
        this(inMemoryOperationLog, cacheUpdater, null);
    }

    /**
     * Creates a new instance of the MemoryStateUpdater class.
     *
     * @param inMemoryOperationLog InMemory Operation Log.
     * @param cacheUpdater         Cache Updater.
     * @param flushCallback        (Optional) A callback to be invoked whenever flush() is invoked.
     */
    MemoryStateUpdater(SequencedItemList<Operation> inMemoryOperationLog, CacheUpdater cacheUpdater, Runnable flushCallback) {
        Preconditions.checkNotNull(cacheUpdater, "cacheUpdater");
        Preconditions.checkNotNull(inMemoryOperationLog, "inMemoryOperationLog");

        this.inMemoryOperationLog = inMemoryOperationLog;
        this.cacheUpdater = cacheUpdater;
        this.flushCallback = flushCallback;
    }

    //endregion

    //region Operations

    /**
     * Puts the Log Updater in Recovery Mode, using the given Metadata Source as interim.
     *
     * @param recoveryMetadataSource The metadata to use during recovery.
     */
    void enterRecoveryMode(ContainerMetadata recoveryMetadataSource) {
        this.cacheUpdater.enterRecoveryMode(recoveryMetadataSource);
    }

    /**
     * Puts the Log Updater out of Recovery Mode, using the given Metadata Source as final.
     *
     * @param successfulRecovery Indicates whether recovery was successful. If not, the operations may be reverted and
     *                           the contents of the memory structures may be cleared out.
     */
    void exitRecoveryMode(boolean successfulRecovery) throws DataCorruptionException {
        this.cacheUpdater.exitRecoveryMode(successfulRecovery);
    }

    /**
     * Appends the given operation.
     *
     * @param operation The operation to append.
     * @throws DataCorruptionException If a serious, non-recoverable, data corruption was detected, such as trying to
     *                                 append operations out of order.
     */
    void process(Operation operation) throws DataCorruptionException {
        if (!operation.canSerialize()) {
            // Nothing to do.
            return;
        }

        // Add entry to MemoryTransactionLog and ReadIndex/Cache. This callback is invoked from the QueueProcessor,
        // which always acks items in order of Sequence Number - so the entries should be ordered (but always check).
        if (operation instanceof StorageOperation) {
            this.cacheUpdater.addToReadIndex((StorageOperation) operation);
            if (operation instanceof StreamSegmentAppendOperation) {
                // Transform a StreamSegmentAppendOperation into its corresponding Cached version.
                try {
                    operation = new CachedStreamSegmentAppendOperation((StreamSegmentAppendOperation) operation);
                } catch (Throwable ex) {
                    if (ExceptionHelpers.mustRethrow(ex)) {
                        throw ex;
                    } else {
                        throw new DataCorruptionException(String.format("Unable to create a CachedStreamSegmentAppendOperation from operation '%s'.", operation), ex);
                    }
                }
            }
        }

        boolean added = this.inMemoryOperationLog.add(operation);
        if (!added) {
            // This is a pretty nasty one. It's safer to shut down the container than continue.
            // We either recorded the Operation correctly, but invoked this callback out of order, or we really
            // recorded the Operation in the wrong order (by sequence number). In either case, we will be inconsistent
            // while serving reads, so better stop now than later.
            throw new DataCorruptionException("About to have added a Log Operation to InMemoryOperationLog that was out of order.");
        }
    }

    /**
     * Flushes recently appended items, if needed.
     * For example, it may trigger Future Reads on the ReadIndex, if the readIndex supports that.
     */
    void flush() {
        this.cacheUpdater.flush();
        if (this.flushCallback != null) {
            this.flushCallback.run();
        }
    }

    //endregion
}
