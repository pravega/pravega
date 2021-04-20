/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.util.AbstractDrainingQueue;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.CacheUtilizationProvider;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.ServiceHaltException;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.ThrottlerSourceListenerCollection;
import io.pravega.segmentstore.storage.cache.CacheFullException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class that allows appending Log Operations to available InMemory Structures.
 */
@ThreadSafe
@Slf4j
class MemoryStateUpdater {
    //region Private

    private final ReadIndex readIndex;
    private final AbstractDrainingQueue<Operation> inMemoryOperationLog;
    private final AtomicBoolean recoveryMode;
    private final ThrottlerSourceListenerCollection readListeners;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MemoryStateUpdater class.
     *
     * @param inMemoryOperationLog InMemory Operation Log.
     * @param readIndex            The ReadIndex to update.
     */
    MemoryStateUpdater(AbstractDrainingQueue<Operation> inMemoryOperationLog, ReadIndex readIndex) {
        this.inMemoryOperationLog = Preconditions.checkNotNull(inMemoryOperationLog, "inMemoryOperationLog");
        this.readIndex = Preconditions.checkNotNull(readIndex, "readIndex");
        this.recoveryMode = new AtomicBoolean();
        this.readListeners = new ThrottlerSourceListenerCollection();
    }

    //endregion

    //region Operations

    /**
     * Registers a {@link ThrottleSourceListener} that will be notified on every Operation Log read.
     *
     * @param listener The {@link ThrottleSourceListener} to register.
     */
    void registerReadListener(@NonNull ThrottleSourceListener listener) {
        this.readListeners.register(listener);
    }

    /**
     * Notifies all registered {@link ThrottleSourceListener} that an Operation Log read has been truncated.
     */
    void notifyLogRead() {
        this.readListeners.notifySourceChanged();
    }

    public int getInMemoryOperationLogSize() {
        return this.inMemoryOperationLog.size();
    }

    /**
     * Gets the {@link CacheUtilizationProvider} shared across all Segment Containers hosted in this process that can
     * be used to query the Cache State.
     *
     * @return The {@link CacheUtilizationProvider}.
     */
    public CacheUtilizationProvider getCacheUtilizationProvider() {
        return this.readIndex.getCacheUtilizationProvider();
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
     * Performs a cleanup of the {@link ReadIndex} by releasing resources allocated for segments that are no longer active
     * and trimming to cache to the minimum essential.
     */
    void cleanupReadIndex() {
        Preconditions.checkState(this.recoveryMode.get(), "cleanupReadIndex can only be performed in recovery mode.");
        this.readIndex.cleanup(null);
        this.readIndex.trimCache();
    }

    /**
     * Processes the given operations and applies them to the ReadIndex and InMemory OperationLog.
     *
     * @param operations An Iterator iterating over the operations to process (in sequence).
     * @param callback   A Consumer that will be invoked on EVERY {@link Operation} in the operations iterator, in the
     *                   order returned from the iterator, regardless of whether the operation was processed or not.
     * @throws ServiceHaltException    If a serious, non-recoverable state was detected, such as unable to create a
     *                                 CachedStreamSegmentAppendOperation.
     * @throws CacheFullException      If any operation in the given iterator contains data that needs to be added to the
     *                                 {@link ReadIndex} but it could not be done due to the cache being full and unable
     *                                 to evict anything to make room for more.
     */
    void process(Iterator<Operation> operations, Consumer<Operation> callback) throws ServiceHaltException, CacheFullException {
        HashSet<Long> segmentIds = new HashSet<>();
        Operation op = null;
        try {
            while (operations.hasNext()) {
                op = operations.next();
                process(op);
                callback.accept(op);
                if (op instanceof SegmentOperation) {
                    // Record recent activity on stream segment, if applicable. This should be recorded for any kind
                    // of Operation that touches a Segment, since when we issue 'triggerFutureReads' on the readIndex,
                    // it should include 'sealed' StreamSegments too - any Future Reads waiting on that Offset will be cancelled.
                    segmentIds.add(((SegmentOperation) op).getStreamSegmentId());
                }
            }
            op = null;
        } catch (Throwable ex) {
            // Invoke the callback on every remaining operation (including the failed one, which is no longer part of the iterator).
            if (op != null) {
                callback.accept(op);
            }
            operations.forEachRemaining(callback);
            throw ex;
        }

        if (!this.recoveryMode.get()) {
            // Trigger Future Reads on those segments which were touched by Appends or Seals.
            this.readIndex.triggerFutureReads(segmentIds);
        }
    }

    /**
     * Processes the given operation and applies it to the ReadIndex and InMemory OperationLog.
     *
     * @param operation The operation to process.
     * @throws ServiceHaltException If a serious, non-recoverable state was detected, such as unable to create a
     *                              CachedStreamSegmentAppendOperation.
     * @throws CacheFullException If the operation contains data that needs to be added to the {@link ReadIndex} but it
     * could not be done due to the cache being full and unable to evict anything to make room for more.
     */
    void process(Operation operation) throws ServiceHaltException, CacheFullException {
        // Add entry to MemoryTransactionLog and ReadIndex/Cache. This callback is invoked from the OperationProcessor,
        // which always acks items in order of Sequence Number - so the entries should be ordered (but always check).
        if (operation instanceof StorageOperation) {
            addToReadIndex((StorageOperation) operation);
            if (operation instanceof StreamSegmentAppendOperation) {
                // Transform a StreamSegmentAppendOperation into its corresponding Cached version.
                StreamSegmentAppendOperation appendOp = (StreamSegmentAppendOperation) operation;
                try {
                    operation = new CachedStreamSegmentAppendOperation(appendOp);
                } catch (Throwable ex) {
                    if (Exceptions.mustRethrow(ex)) {
                        throw ex;
                    } else {
                        throw new ServiceHaltException(String.format("Unable to create a CachedStreamSegmentAppendOperation from operation '%s'.", operation), ex);
                    }
                }

                // Release the memory occupied by this StreamSegmentAppendOperation's BufferView - it has been processed
                // and is no longer needed.
                try {
                    appendOp.close();
                } catch (Throwable ex) {
                    if (Exceptions.mustRethrow(ex)) {
                        throw ex;
                    } else {
                        // We do want to know if for some reason we're unable to release the BufferView's memory, but
                        // this is no reason to halt the ingestion pipeline and cause a container shutdown.
                        log.warn("Unable to release memory for operation '{}': ", operation, ex);
                    }
                }
            }
        }

        try {
            this.inMemoryOperationLog.add(operation);
        } catch (InMemoryLog.OutOfOrderOperationException ex) {
            // This is a pretty nasty one. It's safer to shut down the container than continue.
            // We either recorded the Operation correctly, but invoked this callback out of order, or we really
            // recorded the Operation in the wrong order (by sequence number). In either case, we will be inconsistent
            // while serving reads, so better stop now than later.
            throw new DataCorruptionException("About to have added a Log Operation to InMemoryOperationLog that was out of order.", ex);
        }
    }

    /**
     * Registers the given operation in the ReadIndex.
     *
     * @param operation The operation to register.
     * @throws CacheFullException If the operation could not be added to the {@link ReadIndex} due to the cache being
     * full and unable to evict anything to make room for more.
     * @throws ServiceHaltException If any unexpected exception occurred that prevented the operation from being
     * added to the {@link ReadIndex}. Unexpected exceptions are all exceptions other than those declared in this
     * method or that indicate we are shutting down or that the segment has been deleted.
     */
    private void addToReadIndex(StorageOperation operation) throws ServiceHaltException, CacheFullException {
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
        } catch (CacheFullException ex) {
            // Record the operation that we couldn't add and re-throw the exception as we cannot do anything about it here.
            log.warn("Not adding operation '{}' to ReadIndex because the Cache is full.", operation);
            throw ex;
        } catch (Exception ex) {
            throw new ServiceHaltException(String.format("Unable to add operation '%s' to ReadIndex.", operation), ex);
        }
    }

    //endregion
}