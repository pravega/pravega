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
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.logs.operations.CachedStreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.MergeTransactionOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Helper class that allows appending Log Operations to available InMemory Structures.
 */
@ThreadSafe
class MemoryStateUpdater {
    //region Private

    private final ReadIndex readIndex;
    private final SequencedItemList<Operation> inMemoryOperationLog;
    private final Runnable flushCallback;
    @GuardedBy("readIndex")
    private HashSet<Long> recentStreamSegmentIds;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MemoryStateUpdater class.
     *
     * @param inMemoryOperationLog InMemory Operation Log.
     * @param readIndex            The ReadIndex.
     */
    MemoryStateUpdater(SequencedItemList<Operation> inMemoryOperationLog, ReadIndex readIndex) {
        this(inMemoryOperationLog, readIndex, null);
    }

    /**
     * Creates a new instance of the MemoryStateUpdater class.
     *
     * @param inMemoryOperationLog InMemory Operation Log.
     * @param readIndex            The ReadIndex to update.
     * @param flushCallback        (Optional) A callback to be invoked whenever flush() is invoked.
     */
    MemoryStateUpdater(SequencedItemList<Operation> inMemoryOperationLog, ReadIndex readIndex, Runnable flushCallback) {
        Preconditions.checkNotNull(readIndex, "readIndex");
        Preconditions.checkNotNull(inMemoryOperationLog, "inMemoryOperationLog");

        this.inMemoryOperationLog = inMemoryOperationLog;
        this.readIndex = readIndex;
        this.flushCallback = flushCallback;
        this.recentStreamSegmentIds = new HashSet<>();
    }

    //endregion

    //region Operations

    /**
     * Puts the Log Updater in Recovery Mode, using the given Metadata Source as interim.
     *
     * @param recoveryMetadataSource The metadata to use during recovery.
     */
    void enterRecoveryMode(ContainerMetadata recoveryMetadataSource) {
        this.readIndex.enterRecoveryMode(recoveryMetadataSource);
    }

    /**
     * Puts the Log Updater out of Recovery Mode, using the given Metadata Source as final.
     *
     * @param successfulRecovery Indicates whether recovery was successful. If not, the operations may be reverted and
     *                           the contents of the memory structures may be cleared out.
     */
    void exitRecoveryMode(boolean successfulRecovery) throws DataCorruptionException {
        this.readIndex.exitRecoveryMode(successfulRecovery);
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
            addToReadIndex((StorageOperation) operation);
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
        HashSet<Long> elements;
        synchronized (this.readIndex) {
            elements = this.recentStreamSegmentIds;
            this.recentStreamSegmentIds = new HashSet<>();
        }

        this.readIndex.triggerFutureReads(elements);
        if (this.flushCallback != null) {
            this.flushCallback.run();
        }
    }

    /**
     * Registers the given operation in the ReadIndex.
     *
     * @param operation The operation to register.
     */
    private void addToReadIndex(StorageOperation operation) {
        if (operation instanceof StreamSegmentAppendOperation) {
            // Record a StreamSegmentAppendOperation. Just in case, we also support this type of operation, but we need to
            // log a warning indicating so. This means we do not optimize memory properly, and we end up storing data
            // in two different places.
            StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) operation;
            this.readIndex.append(appendOperation.getStreamSegmentId(), appendOperation.getStreamSegmentOffset(), appendOperation.getData());
        } else if (operation instanceof MergeTransactionOperation) {
            // Record a MergeTransactionOperation. We call beginMerge here, and the StorageWriter will call completeMerge.
            MergeTransactionOperation mergeOperation = (MergeTransactionOperation) operation;
            this.readIndex.beginMerge(mergeOperation.getStreamSegmentId(), mergeOperation.getStreamSegmentOffset(), mergeOperation.getTransactionSegmentId());
        } else {
            assert !(operation instanceof CachedStreamSegmentAppendOperation) : "attempted to add a CachedStreamSegmentAppendOperation to the ReadIndex";
        }

        // Record recent activity on stream segment, if applicable.
        // We should record this for any kind of StorageOperation. When we issue 'triggerFutureReads' on the readIndex,
        // it should include 'sealed' StreamSegments too - any Future Reads waiting on that Offset will be cancelled.
        synchronized (this.readIndex) {
            this.recentStreamSegmentIds.add(operation.getStreamSegmentId());
        }
    }

    //endregion
}
