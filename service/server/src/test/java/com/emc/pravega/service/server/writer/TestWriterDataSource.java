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

package com.emc.pravega.service.server.writer;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.function.CallbackHelpers;
import com.emc.pravega.service.contracts.RuntimeStreamingException;
import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.logs.MemoryOperationLog;
import com.emc.pravega.service.server.logs.operations.MetadataCheckpointOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.testcommon.ErrorInjector;
import com.google.common.base.Preconditions;
import lombok.Setter;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Test version of a WriterDataSource that can accumulate operations in memory (just like the real DurableLog) and only
 * depends on a metadata and a cache as external dependencies.
 * <p>
 * Note that even though it uses an UpdateableContainerMetadata, no changes to this metadata are performed (except recording truncation markers & Sequence Numbers).
 * All other changes (Segment-based) must be done externally.
 */
class TestWriterDataSource implements WriterDataSource, AutoCloseable {
    //region Members

    private final UpdateableContainerMetadata metadata;
    private final MemoryOperationLog log;
    private final Cache cache;
    private Consumer<AcknowledgeArgs> acknowledgeCallback;
    private BiConsumer<Long, Long> completeMergeCallback;
    private final ScheduledExecutorService executor;
    private final DataSourceConfig config;
    private CompletableFuture<Void> addProcessed;
    private long lastAddedCheckpoint;
    private boolean closed;
    @Setter
    private ErrorInjector<Exception> readSyncErrorInjector;
    @Setter
    private ErrorInjector<Exception> readAsyncErrorInjector;
    @Setter
    private ErrorInjector<Exception> ackSyncErrorInjector;
    @Setter
    private ErrorInjector<Exception> ackAsyncErrorInjector;
    @Setter
    private ErrorInjector<Exception> getAppendDataErrorInjector;

    //endregion

    //region Constructor

    TestWriterDataSource(UpdateableContainerMetadata metadata, Cache cache, ScheduledExecutorService executor, DataSourceConfig config) {
        Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(cache, "cache");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkNotNull(config, "config");

        this.metadata = metadata;
        this.cache = cache;
        this.executor = executor;
        this.config = config;
        this.log = new MemoryOperationLog();
        this.lastAddedCheckpoint = 0;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;

            // Cancel any pending adds.
            CompletableFuture<Void> addProcessed;
            synchronized (this.log) {
                addProcessed = this.addProcessed;
                this.addProcessed = null;
            }

            if (addProcessed != null) {
                addProcessed.cancel(true);
            }
        }
    }

    //endregion

    //region add

    public long add(Operation operation) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkArgument(operation.getSequenceNumber() < 0, "Given operation already has a sequence number.");

        // If not a checkpoint op, see if we need to auto-add one.
        boolean isCheckpoint = operation instanceof MetadataCheckpointOperation;
        if (!isCheckpoint) {
            if (this.config.autoInsertCheckpointFrequency != DataSourceConfig.NO_METADATA_CHECKPOINT && this.metadata.getOperationSequenceNumber() - this.lastAddedCheckpoint >= this.config.autoInsertCheckpointFrequency) {
                MetadataCheckpointOperation checkpointOperation = new MetadataCheckpointOperation();
                this.lastAddedCheckpoint = add(checkpointOperation);
            }
        }

        // Set the Sequence Number, after the possible recursive call to add a checkpoint (to maintain Seq No order).
        operation.setSequenceNumber(this.metadata.nextOperationSequenceNumber());

        // We need to record the Truncation Marker/Point prior to actually adding the operation to the log (because it could
        // get picked up very fast by the Writer, so we need to have everything in place).
        if (isCheckpoint) {
            this.metadata.recordTruncationMarker(operation.getSequenceNumber(), operation.getSequenceNumber());
            this.metadata.setValidTruncationPoint(operation.getSequenceNumber());
        }

        if (!this.log.addIf(operation, previous -> previous.getSequenceNumber() < operation.getSequenceNumber())) {
            throw new RuntimeStreamingException(new DataCorruptionException("Sequence numbers out of order."));
        }

        notifyAddProcessed();
        return operation.getSequenceNumber();
    }

    //endregion

    //region WriterDataSource Implementation

    @Override
    public int getId() {
        return this.metadata.getContainerId();
    }

    @Override
    public CompletableFuture<Void> acknowledge(long upToSequenceNumber, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkArgument(this.metadata.isValidTruncationPoint(upToSequenceNumber), "Invalid Truncation Point. Must refer to a MetadataCheckpointOperation.");
        ErrorInjector.throwSyncExceptionIfNeeded(this.ackSyncErrorInjector);

        return ErrorInjector
                .throwAsyncExceptionIfNeeded(this.ackAsyncErrorInjector)
                .thenRunAsync(() -> {
                    this.log.truncate(o -> o.getSequenceNumber() <= upToSequenceNumber);
                    this.metadata.removeTruncationMarkers(upToSequenceNumber);

                    // Invoke the truncation callback.
                    Consumer<AcknowledgeArgs> callback = this.acknowledgeCallback;
                    if (callback != null) {
                        Operation lastOperation = this.log.getLast();
                        long highestSeqNo = lastOperation == null ? upToSequenceNumber : lastOperation.getSequenceNumber();
                        CallbackHelpers.invokeSafely(callback, new AcknowledgeArgs(upToSequenceNumber, highestSeqNo), null);
                    }
                }, this.executor);
    }

    @Override
    public CompletableFuture<Iterator<Operation>> read(long afterSequenceNumber, int maxCount, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        ErrorInjector.throwSyncExceptionIfNeeded(this.readSyncErrorInjector);

        return ErrorInjector
                .throwAsyncExceptionIfNeeded(this.readAsyncErrorInjector)
                .thenCompose(v -> {
                    Iterator<Operation> logReadResult = this.log.read(e -> e.getSequenceNumber() > afterSequenceNumber, maxCount);
                    if (logReadResult.hasNext()) {
                        // Result is readily available; return it.
                        return CompletableFuture.completedFuture(logReadResult);
                    } else {
                        // Result is not yet available; wait for an add and then retry the read.
                        return waitForAdd(afterSequenceNumber, timeout)
                                .thenComposeAsync(v1 -> this.read(afterSequenceNumber, maxCount, timeout), this.executor);
                    }
                });
    }

    @Override
    public void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId) {
        BiConsumer<Long, Long> callback = this.completeMergeCallback;
        if (callback != null) {
            callback.accept(targetStreamSegmentId, sourceStreamSegmentId);
        }
    }

    @Override
    public byte[] getAppendData(CacheKey key) {
        ErrorInjector.throwSyncExceptionIfNeeded(this.getAppendDataErrorInjector);
        return this.cache.get(key);
    }

    @Override
    public boolean isValidTruncationPoint(long operationSequenceNumber) {
        return this.metadata.isValidTruncationPoint(operationSequenceNumber);
    }

    @Override
    public long getClosestValidTruncationPoint(long operationSequenceNumber) {
        return this.metadata.getClosestValidTruncationPoint(operationSequenceNumber);
    }

    @Override
    public void deleteStreamSegment(String streamSegmentName) {
        this.metadata.deleteStreamSegment(streamSegmentName);
    }

    @Override
    public UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) {
        return this.metadata.getStreamSegmentMetadata(streamSegmentId);
    }

    //endregion

    //region Other Properties

    /**
     * Sets a callback that will invoked on every call to acknowledge.
     *
     * @param callback The callback to set.
     */

    public void setAcknowledgeCallback(Consumer<AcknowledgeArgs> callback) {
        this.acknowledgeCallback = callback;
    }

    /**
     * Sets a callback that will invoked on every call to completeMerge.
     *
     * @param callback The callback to set.
     */
    public void setCompleteMergeCallback(BiConsumer<Long, Long> callback) {
        this.completeMergeCallback = callback;
    }

    //endregion

    //region Helpers

    private CompletableFuture<Void> waitForAdd(long currentSeqNo, Duration timeout) {
        CompletableFuture<Void> result;
        synchronized (this.log) {
            if (this.log.size() > 0 && this.log.getLast().getSequenceNumber() > currentSeqNo) {
                // An add has already been processed that meets or exceeds the given sequence number.
                result = CompletableFuture.completedFuture(null);
            } else {
                if (this.addProcessed == null) {
                    // We need to wait for an add, and nobody else is waiting for it too.
                    this.addProcessed = FutureHelpers.futureWithTimeout(timeout, this.executor);
                    FutureHelpers.onTimeout(this.addProcessed, ex -> {
                        synchronized (this.log) {
                            if (this.addProcessed.isCompletedExceptionally()) {
                                this.addProcessed = null;
                            }
                        }
                    });
                }

                result = this.addProcessed;
            }
        }

        return result;
    }

    private void notifyAddProcessed() {
        if (this.addProcessed != null) {
            CompletableFuture<Void> f;
            synchronized (this.log) {
                f = this.addProcessed;
                this.addProcessed = null;
            }

            if (f != null) {
                f.complete(null);
            }
        }
    }

    //endregion

    static class AcknowledgeArgs {
        private final long highestSequenceNumber;
        private final long ackSequenceNumber;

        AcknowledgeArgs(long ackSequenceNumber, long highestSequenceNumber) {
            this.ackSequenceNumber = ackSequenceNumber;
            this.highestSequenceNumber = highestSequenceNumber;
        }

        long getHighestSequenceNumber() {
            return this.highestSequenceNumber;
        }

        long getAckSequenceNumber() {
            return this.ackSequenceNumber;
        }

        @Override
        public String toString() {
            return String.format("AckSeqNo = %d, HighSeqNo = %d", this.ackSequenceNumber, this.highestSequenceNumber);
        }
    }

    static class DataSourceConfig {
        static final int NO_METADATA_CHECKPOINT = -1;
        int autoInsertCheckpointFrequency;
    }
}
