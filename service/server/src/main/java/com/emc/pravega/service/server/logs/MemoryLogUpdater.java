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

import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.google.common.base.Preconditions;

import java.util.HashSet;

/**
 * Helper class that allows appending Log Operations to available InMemory Structures.
 */
public class MemoryLogUpdater {
    //region Private

    private final ReadIndex readIndex;
    private final MemoryOperationLog inMemoryOperationLog;
    private HashSet<Long> recentStreamSegmentIds;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MemoryLogUpdater class.
     *
     * @param inMemoryOperationLog InMemory Operation Log.
     * @param readIndex            ReadIndex.
     */
    public MemoryLogUpdater(MemoryOperationLog inMemoryOperationLog, ReadIndex readIndex) {
        Preconditions.checkNotNull(readIndex, "readIndex");
        Preconditions.checkNotNull(inMemoryOperationLog, "inMemoryOperationLog");

        this.inMemoryOperationLog = inMemoryOperationLog;
        this.readIndex = readIndex;
        this.recentStreamSegmentIds = new HashSet<>();
    }

    //endregion

    //region Operations

    /**
     * Puts the Log Updater in Recovery Mode, using the given Metadata Source as interim.
     *
     * @param recoveryMetadataSource The metadata to use during recovery.
     */
    public void enterRecoveryMode(ContainerMetadata recoveryMetadataSource) {
        this.readIndex.enterRecoveryMode(recoveryMetadataSource);
    }

    /**
     * Puts the Log Updater out of Recovery Mode, using the given Metadata Source as final.
     *
     * @param successfulRecovery Indicates whether recovery was successful. If not, the operations may be reverted and
     *                           the contents of the memory structures may be cleared out.
     */
    public void exitRecoveryMode(boolean successfulRecovery) throws DataCorruptionException {
        this.readIndex.exitRecoveryMode(successfulRecovery);
    }

    /**
     * Appends the given operation.
     *
     * @param operation The operation to append.
     * @throws DataCorruptionException If a serious, non-recoverable, data corruption was detected, such as trying to
     *                                 append operations out of order.
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
     * For example, it may trigger Future Reads on the ReadIndex, if the readIndex supports that.
     */
    public void flush() {
        HashSet<Long> elements;
        synchronized (this.readIndex) {
            elements = this.recentStreamSegmentIds;
            this.recentStreamSegmentIds = new HashSet<>();
        }

        this.readIndex.triggerFutureReads(elements);
    }

    /**
     * Clears all in-memory structures of all data.
     *
     * @throws IllegalStateException If the operation cannot be performed due to the current state of the system, such
     *                               as metadata not being in Recovery mode.
     */
    public void clear() {
        this.readIndex.clear();
        this.inMemoryOperationLog.clear();
        synchronized (this.readIndex) {
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
                this.readIndex.append(appendOperation.getStreamSegmentId(), appendOperation.getStreamSegmentOffset(), appendOperation.getData());
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
                this.recentStreamSegmentIds.add(((StorageOperation) operation).getStreamSegmentId());
            }
        }
    }

    //endregion
}
