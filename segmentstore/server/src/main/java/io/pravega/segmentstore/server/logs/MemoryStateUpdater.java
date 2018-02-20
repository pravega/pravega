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
import io.pravega.common.concurrent.SequentialAsyncProcessor;
import io.pravega.common.util.Retry;
import io.pravega.common.util.SequencedItemList;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.CompletableOperation;
import io.pravega.segmentstore.server.logs.operations.MergeTransactionOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class that allows appending Log Operations to available InMemory Structures.
 */
@ThreadSafe
@Slf4j
class MemoryStateUpdater {
    //region Private

    private final ReadIndex readIndex;
    private final SequencedItemList<Operation> inMemoryOperationLog;
    private final Runnable flushCallback;
    @GuardedBy("pendingCommits")
    private final ArrayDeque<CommitGroup> pendingCommits;
    private final AtomicBoolean recoveryMode;
    private final Supplier<Long> nextId = new AtomicLong(0)::incrementAndGet;
    @GuardedBy("pendingCommits")
    private long commitUpTo = -1;
    private final SequentialAsyncProcessor commitProcessor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MemoryStateUpdater class.
     *
     * @param inMemoryOperationLog InMemory Operation Log.
     * @param readIndex            The ReadIndex to update.
     * @param flushCallback        (Optional) A callback to be invoked whenever flush() is invoked.
     */
    MemoryStateUpdater(SequencedItemList<Operation> inMemoryOperationLog, ReadIndex readIndex, Runnable flushCallback, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(readIndex, "readIndex");
        Preconditions.checkNotNull(inMemoryOperationLog, "inMemoryOperationLog");

        this.inMemoryOperationLog = inMemoryOperationLog;
        this.readIndex = readIndex;
        this.flushCallback = flushCallback;
        this.pendingCommits = new ArrayDeque<>();
        this.recoveryMode = new AtomicBoolean();
        this.commitProcessor = new SequentialAsyncProcessor(this::commitPending, Retry.NO_RETRY, this::logException, executor);
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
     * Creates a new CommitGroup and assigns it an order. All Operations added to this CommitGroup will be processed
     * after all operations of prior CommitGroups and ahead of all operations of subsequent CommitGroups. Within a CommitGroup,
     * operations will be processed in the order in which they are added to it.
     *
     * @param errorHandler A Consumer that will be invoked in case this CommitGroup fails to commit.
     * @return A new CommitGroup.
     */
    CommitGroup beginCommitGroup(Consumer<Throwable> errorHandler) {
        Preconditions.checkState(!this.recoveryMode.get(), "MemoryStateUpdater.CommitGroups are only applicable in non-recovery mode.");
        CommitGroup result = new CommitGroup(this.nextId.get(), errorHandler);
        synchronized (this.pendingCommits) {
            this.pendingCommits.addLast(result);
        }

        return result;
    }

    /**
     * Processes the given operation and applies it to the ReadIndex and InMemory OperationLog.
     *
     * @param operation The operation to process.
     * @throws DataCorruptionException If a serious, non-recoverable, data corruption was detected, such as trying to
     *                                 append operations out of order.
     */
    long process(Operation operation) throws DataCorruptionException {
        long segmentId = -1;
        if (!operation.canSerialize()) {
            // Nothing to do.
            return segmentId;
        }

        // Add entry to MemoryTransactionLog and ReadIndex/Cache. This callback is invoked from the QueueProcessor,
        // which always acks items in order of Sequence Number - so the entries should be ordered (but always check).
        if (operation instanceof StorageOperation) {
            segmentId = addToReadIndex((StorageOperation) operation);
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

        return segmentId;
    }

    private void logException(Throwable ex) {
        log.error("Unhandled exception.", ex);
    }

    /**
     * Commits pending CommitGroups.
     */
    private void commitPending() {
        HashSet<Long> segmentIds = new HashSet<>();

        while (true) {
            // Process all CommitGroups up to, and including the one identified by commitUpTo.
            CommitGroup toProcess;
            synchronized (this.pendingCommits) {
                if (!this.pendingCommits.isEmpty() && this.pendingCommits.peekFirst().id <= this.commitUpTo) {
                    toProcess = this.pendingCommits.removeFirst();
                } else {
                    break;
                }
            }

            // Process all Operations within the CommitGroup in sequence.
            for (CompletableOperation operation : toProcess.operations) {
                try {
                    long segmentId = process(operation.getOperation());
                    if (segmentId >= 0) {
                        segmentIds.add(segmentId);
                    }
                } catch (Throwable ex) {
                    toProcess.commitErrorHandler.accept(ex);
                }
            }
        }

        // Trigger Future Reads on those segments which were touched by Appends or Seals.
        this.readIndex.triggerFutureReads(segmentIds);
        if (this.flushCallback != null) {
            this.flushCallback.run();
        }
    }

    /**
     * Queues the given CommitGroup for committal.
     */
    private void queueForCommit(CommitGroup commitGroup) {
        synchronized (this.pendingCommits) {
            if (!this.pendingCommits.isEmpty()) {
                Preconditions.checkArgument(commitGroup.id <= this.pendingCommits.peekLast().id,
                        "Invalid CommitGroup (Id = %s).", commitGroup.id);
            }
            this.commitUpTo = Math.max(this.commitUpTo, commitGroup.id);
        }

        this.commitProcessor.runAsync();
    }

    /**
     * Registers the given operation in the ReadIndex.
     *
     * @param operation The operation to register.
     */
    private long addToReadIndex(StorageOperation operation) {
        if (operation instanceof StreamSegmentAppendOperation) {
            // Record a StreamSegmentAppendOperation. Just in case, we also support this type of operation, but we need to
            // log a warning indicating so. This means we do not optimize memory properly, and we end up storing data
            // in two different places.
            StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) operation;
            this.readIndex.append(appendOperation.getStreamSegmentId(),
                    appendOperation.getStreamSegmentOffset(),
                    appendOperation.getData());
        } else if (operation instanceof MergeTransactionOperation) {
            // Record a MergeTransactionOperation. We call beginMerge here, and the StorageWriter will call completeMerge.
            MergeTransactionOperation mergeOperation = (MergeTransactionOperation) operation;
            this.readIndex.beginMerge(mergeOperation.getStreamSegmentId(),
                    mergeOperation.getStreamSegmentOffset(),
                    mergeOperation.getTransactionSegmentId());
        } else {
            assert !(operation instanceof CachedStreamSegmentAppendOperation)
                    : "attempted to add a CachedStreamSegmentAppendOperation to the ReadIndex";
        }

        // Record recent activity on stream segment, if applicable.
        // We should record this for any kind of StorageOperation. When we issue 'triggerFutureReads' on the readIndex,
        // it should include 'sealed' StreamSegments too - any Future Reads waiting on that Offset will be cancelled.
        return operation.getStreamSegmentId();
    }

    //endregion

    //region CommitGroup

    @NotThreadSafe
    @RequiredArgsConstructor
    class CommitGroup {
        private final long id;
        private final Consumer<Throwable> commitErrorHandler;
        private List<CompletableOperation> operations = new ArrayList<>();

        /**
         * Adds the given CompletableOperation to this CommitGroup.
         *
         * @param operation The CompletableOperation to add.
         */
        void add(CompletableOperation operation) {
            this.operations.add(operation);
        }

        /**
         * Completes all CompletableOperations in this CommitGroup, then queues it up for committal. After this method
         * is invoked, no further calls to add() are allowed.
         */
        Collection<CompletableOperation> commit() {
            this.operations = Collections.unmodifiableList(this.operations);
            this.operations.forEach(CompletableOperation::complete);
            MemoryStateUpdater.this.queueForCommit(this);
            return this.operations;
        }
    }

    //endregion
}