/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.util.SequencedItemList;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.CacheUtilizationProvider;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class that allows appending Log Operations to available InMemory Structures.
 */
@ThreadSafe
@Slf4j
class MemoryStateUpdater implements CacheUtilizationProvider {
    //region Private

    private final ReadIndex readIndex;
    private final SequencedItemList<Operation> inMemoryOperationLog;
    private final Runnable commitSuccess;
    private final AtomicBoolean recoveryMode;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MemoryStateUpdater class.
     *
     * @param inMemoryOperationLog InMemory Operation Log.
     * @param readIndex            The ReadIndex to update.
     * @param commitSuccess        (Optional) A callback to be invoked whenever an Operation or set of Operations are
     *                             successfully committed.
     */
    MemoryStateUpdater(SequencedItemList<Operation> inMemoryOperationLog, ReadIndex readIndex, Runnable commitSuccess) {
        this.inMemoryOperationLog = Preconditions.checkNotNull(inMemoryOperationLog, "inMemoryOperationLog");
        this.readIndex = Preconditions.checkNotNull(readIndex, "readIndex");
        this.commitSuccess = commitSuccess;
        this.recoveryMode = new AtomicBoolean();
    }

    //endregion

    //region Operations

    @Override
    public double getCacheUtilization() {
        return this.readIndex.getCacheUtilization();
    }

    /**
     * Puts the Log Updater in Recovery Mode, using the given Metadata Source as interim.
     *
     * @param recoveryMetadataSource The metadata to use during recovery.
     */
    void enterRecoveryMode(ContainerMetadata recoveryMetadataSource) {
        this.readIndex.enterRecoveryMode(recoveryMetadataSource);
        this.recoveryMode.set(true);
    }

    /**
     * Puts the Log Updater out of Recovery Mode, using the given Metadata Source as final.
     *
     * @param successfulRecovery Indicates whether recovery was successful. If not, the operations may be reverted and
     *                           the contents of the memory structures may be cleared out.
     */
    void exitRecoveryMode(boolean successfulRecovery) throws DataCorruptionException {
        this.readIndex.exitRecoveryMode(successfulRecovery);
        this.recoveryMode.set(false);
    }

    /**
     * Processes the given operations and applies them to the ReadIndex and InMemory OperationLog.
     *
     * @param operations An Iterator iterating over the operations to process (in sequence).
     * @throws DataCorruptionException If a serious, non-recoverable, data corruption was detected, such as trying to
     *                                 append operations out of order.
     */
    void process(Iterator<Operation> operations) throws DataCorruptionException {
        HashSet<Long> segmentIds = new HashSet<>();
        while (operations.hasNext()) {
            Operation op = operations.next();
            process(op);
            if (op instanceof SegmentOperation) {
                // Record recent activity on stream segment, if applicable. This should be recorded for any kind
                // of Operation that touches a Segment, since when we issue 'triggerFutureReads' on the readIndex,
                // it should include 'sealed' StreamSegments too - any Future Reads waiting on that Offset will be cancelled.
                segmentIds.add(((SegmentOperation) op).getStreamSegmentId());
            }
        }

        if (!this.recoveryMode.get()) {
            // Trigger Future Reads on those segments which were touched by Appends or Seals.
            this.readIndex.triggerFutureReads(segmentIds);
            if (this.commitSuccess != null) {
                this.commitSuccess.run();
            }
        }
    }

    /**
     * Processes the given operation and applies it to the ReadIndex and InMemory OperationLog.
     *
     * @param operation The operation to process.
     * @throws DataCorruptionException If a serious, non-recoverable, data corruption was detected, such as trying to
     *                                 append operations out of order.
     */
    void process(Operation operation) throws DataCorruptionException {
        if (!operation.canSerialize()) {
            // Nothing to do.
            return;
        }

        // Add entry to MemoryTransactionLog and ReadIndex/Cache. This callback is invoked from the OperationProcessor,
        // which always acks items in order of Sequence Number - so the entries should be ordered (but always check).
        if (operation instanceof StorageOperation) {
            addToReadIndex((StorageOperation) operation);
            if (operation instanceof StreamSegmentAppendOperation) {
                // Transform a StreamSegmentAppendOperation into its corresponding Cached version.
                try {
                    operation = new CachedStreamSegmentAppendOperation((StreamSegmentAppendOperation) operation);
                } catch (Throwable ex) {
                    if (Exceptions.mustRethrow(ex)) {
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
     * Registers the given operation in the ReadIndex.
     *
     * @param operation The operation to register.
     */
    private void addToReadIndex(StorageOperation operation) {
        try {
            if (operation instanceof StreamSegmentAppendOperation) {
                // Record a StreamSegmentAppendOperation. Just in case, we also support this type of operation, but we need to
                // log a warning indicating so. This means we do not optimize memory properly, and we end up storing data
                // in two different places.
                StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) operation;
                this.readIndex.append(appendOperation.getStreamSegmentId(),
                        appendOperation.getStreamSegmentOffset(),
                        appendOperation.getData());
            } else if (operation instanceof MergeSegmentOperation) {
                // Record a MergeSegmentOperation. We call beginMerge here, and the StorageWriter will call completeMerge.
                MergeSegmentOperation mergeOperation = (MergeSegmentOperation) operation;
                this.readIndex.beginMerge(mergeOperation.getStreamSegmentId(),
                        mergeOperation.getStreamSegmentOffset(),
                        mergeOperation.getSourceSegmentId());
            } else {
                assert !(operation instanceof CachedStreamSegmentAppendOperation)
                        : "attempted to add a CachedStreamSegmentAppendOperation to the ReadIndex";
            }
        } catch (ObjectClosedException | StreamSegmentNotExistsException ex) {
            // The Segment is in the process of being deleted. We usually end up in here because a concurrent delete
            // request has updated the metadata while we were executing.
            log.warn("Not adding operation '{}' to ReadIndex because it refers to a deleted StreamSegment.", operation);
        }
    }

    //endregion
}